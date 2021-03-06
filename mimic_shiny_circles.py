from blessings import Terminal
import boto3
from boto3.dynamodb.types import TypeDeserializer
import ctypes
import inquirer
from inquirer import errors
import json
import multiprocessing
import subprocess
import sys
import tempfile
import time
import yaml

import rip_cd


EJECT = '/usr/bin/eject'


def validate_disc_number(answers, current):
    if current == '':
        answers['disc_number'] = 1
        answers['disc_count'] = 1
    else:
        try:
            (disc_number, disc_count) = current.split('/')
            answers['disc_number'] = int(disc_number)
            answers['disc_count'] = int(disc_count)
        except ValueError:
            raise errors.ValidationError('', reason='Needs to be either "x/y" or blank (for 1/1)')

    return True


class disc_info():

    def __init__(self):
        self.disc_title = None
        self.disc_number = None
        self.disc_count = None
        self.years = None
        self.companies = None

    def input(self):
        questions = [
            inquirer.Text('disc_title', message="Disc title"),
            inquirer.Text('', message="Disc number (x/y; press enter if not part of a set)", validate=validate_disc_number),
            inquirer.Text('years', message="Years mentioned on disc (separated with semi-colons or hyphen)"),
            inquirer.Text('companies', message="Companies mentioned on disc (separated with semi-colons)")
        ]
        answers = inquirer.prompt(questions)

        self.disc_title = answers['disc_title']
        self.disc_number = answers['disc_number']
        self.disc_count = answers['disc_count']
        self.years = answers['years']
        self.companies = answers['companies']

    def show(self):
        print("disc title: " + self.disc_title)
        if self.disc_number:
            print("disc %d / %d" % (self.disc_number, self.disc_count))
        else:
            print("(single disc)")
        print("Years mentioned on disc: " + self.years)
        print("Companies mentioned on disc: " + self.companies)

    def get_dict(self):
        return {
            'disc_title': self.disc_title,
            'disc_number': self.disc_number,
            'disc_count': self.disc_count,
            'years': self.years,
            'companies': self.companies
        }


def create_progress_string(progress_dict):
    percent = 100 * progress_dict['current_track_bytes'] / progress_dict['current_track_total_bytes']
    track_progress_str = 'track %d / %d: %d / %d bytes (%d%%) [ ' % (progress_dict['current_track'], progress_dict['track_count'], progress_dict['current_track_bytes'], progress_dict['current_track_total_bytes'], percent)
    track_progress_str_len = len(track_progress_str)
    bar_width = term.width - track_progress_str_len - 2
    filled_width = int(1.0 * percent * bar_width / 100)
    track_progress_str += '>' * filled_width
    track_progress_str += ' ' * (bar_width - filled_width) + ' ]' 

    return track_progress_str


# Rip a pair of discs simultaneously.
#
# Returns True if both of the rips completed successfully; False otherwise.
def rip_pair(term, root_rip_dir, drive1, drive2, second_pass=False):
    pass_count = 1
    if second_pass:
        pass_count = 2

    disc1_dir1 = root_rip_dir + '/' + 'disc1-dir%d' % (pass_count)
    disc2_dir1 = root_rip_dir + '/' + 'disc2-dir%d' % (pass_count)

    manager = multiprocessing.Manager()
    parent_comm1, child_comm1 = multiprocessing.Pipe()
    return_string1 = manager.Value(ctypes.c_char_p, '')
    extraction_process1 = multiprocessing.Process(
        target=rip_cd.extract_cd,
        args=(drive1['device'], disc1_dir1, child_comm1, return_string1),
        name='Extraction process #1 (pass %d)' % (pass_count)
    )
    extraction_process1.start()

    parent_comm2, child_comm2 = multiprocessing.Pipe()
    return_string2 = manager.Value(ctypes.c_char_p, '')
    extraction_process2 = multiprocessing.Process(
        target=rip_cd.extract_cd,
        args=(drive2['device'], disc2_dir1, child_comm2, return_string2),
        name='Extraction process #2 (pass %d)' % (pass_count)
    )
    extraction_process2.start()

    with term.fullscreen():
        with term.location(0, 1):
            print("Extracting disc using '%s'..." % (drive1['description']))
        with term.location(0, 6):
            print("Extracting disc using '%s'..." % (drive2['description']))

        while extraction_process1.is_alive() or extraction_process2.is_alive():
            if parent_comm1.poll(1):
                with term.location(0, 3):
                    progress_dict = parent_comm1.recv()
                    print(create_progress_string(progress_dict))

            if parent_comm2.poll(1):
                with term.location(0, 8):
                    progress_dict = parent_comm2.recv()
                    print(create_progress_string(progress_dict))

            if return_string1.value != "":
                print("Failure seen on '%s'; shutting down '%s'..." % (drive1['description'], drive2['description']))
                parent_comm2.send("shutdown")
            elif return_string2.value != "":
                print("Failure seen on '%s'; shutting down '%s'..." % (drive2['description'], drive1['description']))
                parent_comm1.send("shutdown")

        error = False
        if return_string1.value != "":
            print("****** Problem extracting disc in '%s'" % (drive1['description']))
            print(return_string1.value)
            print()
            error = True
        if return_string2.value != "":
            print("****** Problem extracting disc in '%s'" % (drive2['description']))
            print(return_string2.value)
            print()
            error = True

        if second_pass:
            input("Press Enter to eject and continue...")
            subprocess.run([EJECT, drive1['device']])
            subprocess.run([EJECT, drive2['device']])
        else:
            subprocess.run([EJECT, drive1['device']])
            subprocess.run([EJECT, drive2['device']])
            if error:
                print("Encountered error; exiting")
            else:
                print("Finished first pass; swap and press Enter...")
            input("press Enter...")

    return not error


def get_disc_info(term, drive_dict, dbd_client):
    serializer = boto3.dynamodb.types.TypeSerializer()

    info = disc_info()

    with term.fullscreen():
        with term.location(0, 1):
            print("Input information about the disc to be placed in '" + drive_dict['description'] + "'")
            print()
            while 1:
                # request input from user
                info.input()
                print()
                # show the user what they just typed
                info.show()
                print()
                answer = input("Is the disc information correct? [Y/n] ")
                if answer == 'n':
                    continue

                # check whether this would overwrite an entry already in the database
                print("Checking whether disc is already in the database...")
                disc_dict = info.get_dict()
                if disc_dict['disc_number'] == None:
                    disc_dict['disc_number'] = 1
                disc_db_key = { 'disc_title': disc_dict['disc_title'], 'disc_number': disc_dict['disc_number'] }
                disc_db_item = {k: serializer.serialize(v) for k,v in disc_db_key.items() }
                response = dbd_client.get_item(TableName='disc_table', Key=disc_db_item)
                if 'Item' in response:
                    print("Problem: '%s', disc #%d already exists in the database" % (disc_dict['disc_title'], disc_dict['disc_number']))
                    input("Press Enter to input information about a new disc...")
                    continue

                print()
                input("Insert disc into '" + drive_dict['description'] + "' and press Enter...")
                (status, msg) = rip_cd.verify_cd(drive_dict['device'])
                if not status:
                    print("Problem with this disc: " + msg)
                    input("Press Enter to input information about a new disc...")
                    continue

                # if flow made it this far, everything is okay
                break

    return info


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("USAGE: mimic_shiny_circles.py <disc pair configuration file>")
        sys.exit(1)
    
    # parse the configuration file
    config = yaml.safe_load(open(sys.argv[1], 'r').read())
    drive_pair = config['Drive Pair']

    # check AWS credentials up front
    aws = config.get('AWS')
    pd_queue = None
    if aws:
        # establish SQS stuff
        sqs_client = boto3.client(
            'sqs',
            region_name=config['AWS']['Region'],
            aws_access_key_id=config['AWS']['Access Key'],
            aws_secret_access_key=config['AWS']['Secret Key']
        )
        sqs_resource = boto3.resource(
            'sqs',
            region_name=config['AWS']['Region'],
            aws_access_key_id=config['AWS']['Access Key'],
            aws_secret_access_key=config['AWS']['Secret Key']
        )

        pd_queue_url = sqs_client.get_queue_url(QueueName=config['AWS']['Queue'])
        pd_queue = sqs_resource.Queue(pd_queue_url['QueueUrl'])

        # connect to DynamoDB
        print("Connecting to DynamoDB...")
        dbd_client = boto3.client(
            'dynamodb',
            region_name=config['AWS']['Region'],
            aws_access_key_id=config['AWS']['Access Key'],
            aws_secret_access_key=config['AWS']['Secret Key']
        )


    term = Terminal()
    disc1_info = get_disc_info(term, drive_pair[0], dbd_client)
    disc2_info = get_disc_info(term, drive_pair[1], dbd_client)

    metadata = {
        'disc1': disc1_info.get_dict(),
        'disc2': disc2_info.get_dict()
    }

    # generate root directory that will hold 2 pairs of rips
    root_rip_dir = tempfile.mkdtemp(prefix='disc-pairs-', dir=config.get('Output Directory'))
    print("Extracting images to '%s'..." % (root_rip_dir))

    # generate the main metadata file
    metadata_filename = root_rip_dir + '/metadata.json'
    f = open(metadata_filename, 'w')
    f.write(json.dumps(metadata, indent=4))
    f.close()

    # perform the rips
    if not rip_pair(term, root_rip_dir, drive_pair[0], drive_pair[1]):
        print("Something failed during the first pair rip; exiting (remember to clean up temporary directory '%s')" % root_rip_dir)
        sys.exit(1)
    if not rip_pair(term, root_rip_dir, drive_pair[1], drive_pair[0], second_pass=True):
        print("Something failed during the second pair rip; exiting (remember to clean up temporary directory '%s')" % root_rip_dir)
        sys.exit(1)

    # log the root rip dir to the processing queue
    if pd_queue:
        response = pd_queue.send_message(MessageBody=root_rip_dir)
        if not response.get('ResponseMetadata') or not response['ResponseMetadata'].get('HTTPStatusCode') or response['ResponseMetadata']['HTTPStatusCode'] != 200:
            print("There was a problem sending a message to the SQS service")
            print(response)
            print()
            print("Note that there is now an orphaned directory: " + root_rip_dir)
        else:
            print("Logged directory in queue")

    print("Finished")
