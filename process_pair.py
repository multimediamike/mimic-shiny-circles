import boto3
from boto3.dynamodb.types import TypeDeserializer
import json
import os
import shutil
import string
import sys
import time
import yaml


WAIT_SECONDS = 20


# taken from https://gist.github.com/seanh/93666
def format_filename(s):
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in s if c in valid_chars)
    filename = filename.replace(' ','_') # I don't like spaces in filenames.
    return filename


# Compare the contents of 2 track manifests. Returns a tuple. The first
# element is a Boolean indicating whether checksums and track counts
# are identical.
#
# If False, the second parameter contains an error message.
#
# If True, the second parameter contains a dictionary of information to
# be logged to the database.
def compare_manifests(manifest1_filename, manifest2_filename):
    manifest1 = json.loads(open(manifest1_filename, 'r').read())
    manifest2 = json.loads(open(manifest2_filename, 'r').read())

    if len(manifest1['tracks']) != len(manifest2['tracks']):
        error = "manifests have different track counts: %d != %d\n" % (manifest1['tracks'], manifest2['tracks'])
        return (False, error)

    track_info = []
    drive_info = [{
        'device': manifest1['device'],
        'drive_string': manifest1['drive_string'],
        'start_time_utc': manifest1['start_time_utc'],
        'end_time_utc': manifest1['end_time_utc'],
        'time_in_seconds': manifest1['time_in_seconds'],
        'read_performance': []
    }, {
        'device': manifest2['device'],
        'drive_string': manifest2['drive_string'],
        'start_time_utc': manifest2['start_time_utc'],
        'end_time_utc': manifest2['end_time_utc'],
        'time_in_seconds': manifest2['time_in_seconds'],
        'read_performance': []
    }]

    for i in range(len(manifest1['tracks'])):
        track1 = manifest1['tracks'][i]
        track2 = manifest2['tracks'][i]
        if track1['sha1'] != track2['sha1']:
            error = "track %d has different checksums:\n"
            error += "  1) %d bytes, sha1 = %s\n" % (track1['size'], track1['sha1'])
            error += "  2) %d bytes, sha1 = %s" % (track2['size'], track2['sha1'])
            return (False, error)
        track_info.append({
            'track_number': track1['track_number'],
            'type': track1['type'],
            'data_type': track1['data_type'],
            'filename': track1['path'][track1['path'].rfind('/')+1:],
            'size': track1['size'],
            'sector_size': track1['sector_size'],
            'sector_count': track1['sector_count'],
            'sha1': track1['sha1']
        })
        drive_info[0]['read_performance'].append(track1['size_progress'])
        drive_info[1]['read_performance'].append(track2['size_progress'])

    return (True, (track_info, drive_info))


def organize_metadata(metadata):
    db_item = {
        'disc_title': metadata['disc_title'],
        'disc_number': 1 if not metadata['disc_number'] else metadata['disc_number'],
        'disc_count': 1 if not metadata['disc_count'] else metadata['disc_count']
    }
    if metadata['years']:
        db_item['years'] = metadata['years']
    if metadata['companies']:
        db_item['companies'] = metadata['companies']
    return(db_item)


# Returns a Boolean which is True if the response indicates success.
def aws_succeeded(resp):
    if resp.get('ResponseMetadata') and resp['ResponseMetadata'].get('HTTPStatusCode') == 200:
        return True
    else:
        return False


def process_directory(input_path, archive_dir, dbd_client):
    serializer = boto3.dynamodb.types.TypeSerializer()

    # make sure the input path exists
    if not os.path.exists(input_path):
        print(input_path + " does not exist")
        return

    # load metadata
    metadata = json.loads(open(input_path + '/metadata.json').read())

    # process 2 pairs
    both_pairs_succeeded = True
    for i in [1, 2]:
        disc_key = 'disc' + str(i)
        dir_name = metadata[disc_key]['disc_title']
        if metadata[disc_key]['disc_number']:
            dir_name += ' - disc ' + str(metadata[disc_key]['disc_number'])
        dir_name = format_filename(dir_name)
        full_output_path = archive_dir + '/' + dir_name
        print("Processing '" + metadata[disc_key]['disc_title'] + "' into directory '" + full_output_path + "'")
        if os.path.exists(full_output_path):
            print("Output directory '%s' already exists; skipping item" % (full_output_path))
            continue

        # compare manifests
        (status, data) = compare_manifests(
            input_path + '/' + disc_key + '-dir1/track-manifest.json',
            input_path + '/' + disc_key + '-dir2/track-manifest.json',
        )
        if status:
            (tracks, drive_info) = data

            # log the track info
            db_item = organize_metadata(metadata[disc_key])
            db_item['directory'] = dir_name
            db_item['tracks'] = tracks
            dbd_item = {k: serializer.serialize(v) for k,v in db_item.items()} 
            response = dbd_client.put_item(TableName='disc_table', Item=dbd_item)
            if not aws_succeeded(response):
                print("Received an AWS error:")
                print(response)
                both_pairs_succeeded = False
                continue

            # log the drive info dictionaries
            for drive_item in drive_info:
                drive_item['disc_directory'] = dir_name
                dbd_item = {k: serializer.serialize(v) for k,v in drive_item.items()} 
                response = dbd_client.put_item(TableName='drive_table', Item=dbd_item)
                if not aws_succeeded(response):
                    print("Received an AWS error:")
                    print(response)
                    both_pairs_succeeded = False
                    continue

            # move the contents of one of the directories to its final spot
            shutil.move(input_path + '/' + disc_key + '-dir1', full_output_path)
        else:
            both_pairs_succeeded = False

    # if all the data underneath the top level directory was successfully
    # processed, remove the top level directory
    if both_pairs_succeeded:
        print("removing directory '" + input_path + "'...")
        shutil.rmtree(input_path)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("USAGE: process_pair.py <configuration file>")
        sys.exit(1)
    
    # parse the configuration file
    config = yaml.safe_load(open(sys.argv[1], 'r').read())
    archive_dir = config['Archive Directory']

    # validate that the final archive directory exists and is writable
    if not os.path.exists(archive_dir):
        print(archive_dir + " does not exist")
        sys.exit(1)
    if not os.path.isdir(archive_dir):
        print(archive_dir + " is not a directory")
        sys.exit(1)
    if not os.access(archive_dir, os.X_OK):
        print(archive_dir + " is not writable")
        sys.exit(1)

    # connect to the AWS simple queue service
    aws = config.get('AWS')
    print("Connecting to SQS...")
    sqs_client = boto3.client(
        'sqs',
        region_name=config['AWS']['Region'],
        aws_access_key_id=config['AWS']['Access Key'],
        aws_secret_access_key=config['AWS']['Secret Key']
    )

    # connect to DynamoDB
    print("Connecting to DynamoDB...")
    dbd_client = boto3.client(
        'dynamodb',
        region_name=config['AWS']['Region'],
        aws_access_key_id=config['AWS']['Access Key'],
        aws_secret_access_key=config['AWS']['Secret Key']
    )

    pd_queue_url = sqs_client.get_queue_url(QueueName=config['AWS']['Queue'])['QueueUrl']

    # while there are messages in the queue, process directory messages
    print("Waiting for messages...")
    while True:
        # get a message
        sqs_response = sqs_client.receive_message(
            QueueUrl=pd_queue_url,
            WaitTimeSeconds=WAIT_SECONDS,
            MaxNumberOfMessages=1
        )
        metadata = sqs_response['ResponseMetadata']
        if metadata.get('HTTPStatusCode') != 200:
            print("Message did not return HTTP status 200")
            continue
        if 'Messages' not in sqs_response:
            continue
        message = sqs_response['Messages'][0]
        receipt_handle = message['ReceiptHandle']

        # process the message
        print("Processing " + message['Body'])
        process_directory(message['Body'], archive_dir, dbd_client)

        # remember to delete message
        print("Processing finished; deleting message from queue...")
        sqs_client.delete_message(
            QueueUrl=pd_queue_url,
            ReceiptHandle=receipt_handle
        )
        print("Waiting for messages...")
