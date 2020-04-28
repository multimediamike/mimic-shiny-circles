import ctypes
import datetime
import hashlib
import json
import multiprocessing
import os
import subprocess
import sys
import tempfile
import time
import yaml


# commands
LSSCSI = '/usr/bin/lsscsi'
CDINFO2JSON = './cdinfo2json'
DD = '/bin/dd'
CDPARANOIA = '/usr/bin/cdparanoia'


# constants
WAV_HEADER_SIZE = 44
AUDIO_BYTES_PER_SECTOR = 2352
FORM1_DATA_BYTES_PER_SECTOR = 2048


def get_drive_string(input_device):
    # derive the drive string using the 'lsscsi' command
    ret = subprocess.run(LSSCSI, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if ret.returncode != 0:
        return None
    lsscsi_output = ret.stdout

    # if the specified device is actually a symbolic link, find the actual device
    device_path = os.path.realpath(input_device)

    # iterate over the output of 'lsscsi' and find the specified device
    drive_string = None
    for line in lsscsi_output.splitlines():
        candidate_device = line[53:].strip().decode('utf-8')
        if device_path == candidate_device:
            drive_string = line[30:47].strip().decode('utf-8')

    return drive_string


# Attempts to read the CD table of contents from the drive indicated by
# input_device.
#
# Returns a 2-tuple. The first element of the tuple is a Boolean indicating
# True for success or False for failure.
#
# In the case of failure, the second element is a string describing
# the error. In the case of success, the second element is a data
# structure describing the CD's table of contents.
def read_cd_toc(input_device):
    # verify that the tool exists and is executable
    if not os.path.exists(CDINFO2JSON):
        return (False, "CD ToC tool ('" + CDINFO2JSON + "') was not found")
    if not os.access(CDINFO2JSON, os.X_OK):
        return (False, "CD ToC tool ('" + CDINFO2JSON + "') is not executable")
    ret = subprocess.run([CDINFO2JSON, input_device], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if ret.returncode != 0:
        return (False, "CD ToC tool ('" + CDINFO2JSON + "') returned status code %d (expected 0)" % (ret.returncode))
    try:
        cd_toc = json.loads(ret.stdout)
    except:
        return (False, "CD ToC tool ('" + CDINFO2JSON + "') returned invalid JSON\n" + ret.stdout.decode('utf-8'))

    return (True, cd_toc)


# Verify whether the CD inserted into input_device can be extracted, per
# policy.
#
# Returns a tuple of (Boolean, string): The Boolean is True if the CD can
# be extracted. If it can't be extracted, the Boolean is False and the
# string contains an error describing why the CD can't be extracted.
def verify_cd(input_device):
    (status, data) = read_cd_toc(input_device)
    if not status:
        return (False, data)
    cd_toc = data

    # make sure the input dictionary has at least 1 element
    if cd_toc['track_count'] <= 0:
        return (False, "No tracks found")
    if cd_toc['track_count'] != len(cd_toc['tracks']):
        return (False, "Corrupted ToC (is the disc inserted?)")

    # iterate over the table of contents and determine whether it's suitable
    tracks = cd_toc['tracks']

    # expect the first track to be a form 1 data track (either mode 1 or 2 is fine)
    if tracks[0]['track_type'] != 'data':
        return (False, "Expected first track to be a data track (found %s)" % (tracks[0]['track_type']))
    data_type = tracks[0].get('data_type')
    if data_type != 'mode 1' and not tracks[0].get('data_type').endswith('form 1'):
        return (False, "Expected first track to contain form 1 data (found '%s')" % (tracks[0].get('data_type')))

    # traverse over any remaining tracks and ensure they are all audio tracks
    for i in range(1, len(tracks)):
        if tracks[i]['track_type'] != 'audio':
            return (False, "Expected tracks after the first track to be audio; track %d is '%s'" % (i+2, tracks[i]['track_type']))

    # for now, don't bother with discs that have audio tracks; if the
    # code flow has gotten this far, then if the disc has more than 1
    # track, there are audio tracks
    if len(tracks) > 1:
        return (False, "CD has audio tracks")

    return (True, "CD can be extracted")


def run_from_other_process(cmd_tuple, return_string):
    ret = subprocess.run(cmd_tuple, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if ret.returncode != 0:
        return_string.value = "Non-zero return code"


# returns an array of filesizes at 1-second intervals
def watch_file_size(process, filename, progress_dict, comms=None):
    sizes = []
    while process.is_alive():
        if os.path.exists(filename):
            sizes.append(os.path.getsize(filename))
            progress_dict['current_track_bytes'] = sizes[-1]
        if comms:
            comms.send(progress_dict)
        time.sleep(1)

    # send one last progress event as a '100%' notification
    sizes.append(os.path.getsize(filename))
    progress_dict['current_track_bytes'] = sizes[-1]
    if comms:
        comms.send(progress_dict)

    return sizes


def checksum_track(filename):
    size = os.path.getsize(filename)
    f = open(filename, 'rb')
    sha = hashlib.sha1()
    while size > 0:
        if size < 1024 * 1024:
            bytes_to_read = size
        else:
            bytes_to_read = 1024 * 1024
        data = f.read(bytes_to_read)
        size -= bytes_to_read
        sha.update(data)

    return sha.hexdigest()


# Given an optical device, extract the individual data and audio tracks
# to the path specified by output_directory.
#
# If comm parameter is not None, it is expected to be of type
# multiprocessing.Pipe. If this exists, it is assumed that this function
# is being run from a separate process. Every second or so, this pipe
# will communicate its extraction progress. Further, it will listen for
# cancel commands to halt the current extraction process.
#
# The fourth parameter is of type multiprocessing.Value and contains an
# empty string upon entry. If the extraction process is successful, the
# string is still empty when the function exists. Otherwise, it is updated
# with a string describing the error encountered.
def extract_cd(input_device, output_directory, comm, return_string):
    # determine the drive string
    drive_string = get_drive_string(input_device)

    # validate that the CD meets policy
    (status, error) = verify_cd(input_device)
    if not status:
        return_string.value = "Error: verify_cd returned " + error
        return

    # get the CD's table of contents
    (status, data) = read_cd_toc(input_device)
    if not status:
        return_string.value = "Error: could not read disc's table of contents: " + data
        return
    cd_toc = data

    # tally the total bytes to be extracted
    total_bytes = FORM1_DATA_BYTES_PER_SECTOR * cd_toc['tracks'][0]['sector_count']
    for track in range(1, cd_toc['track_count']):
        total_bytes += WAV_HEADER_SIZE + cd_toc['tracks'][track]['sector_count'] * AUDIO_BYTES_PER_SECTOR

    # prepare progress-tracking data structure
    progress_dict = {
        'total_bytes_extracted': 0,
        'total_bytes': total_bytes,
        'current_track': 1,
        'track_count': cd_toc['track_count'],
        'current_track_bytes': 0,
        'current_track_total_bytes': 0
    }

    # attempt to create output directory, but only if it's not already there
    if os.path.exists(output_directory):
        manifest_filename = output_directory + '/track-manifest.json'
        if os.path.exists(manifest_filename):
            return_string.value = "Error: path already exists: '%s'" % (output_directory)
            return
    else:
        try:
            os.makedirs(output_directory)
        except:
            return_string.value = "Error: could not create directory: '%s'" % (output_directory)
            return

    # start building the track manifest
    track_manifest = {
        'device': input_device,
        'drive_string': drive_string,
        'start_time_utc': datetime.datetime.utcnow().strftime("%a %b %d %H:%M:%S %Z %Y"),
        'tracks': []
    }
    start_time = int(time.time())

    # use 'dd' to extract the data track
    progress_dict['current_track_bytes'] = 0
    progress_dict['current_track_total_bytes'] = cd_toc['tracks'][0]['sector_count'] * FORM1_DATA_BYTES_PER_SECTOR
    data_track = output_directory + '/01-data-track.iso'
    dd_process = multiprocessing.Process(target=run_from_other_process, args=([DD, 'if=' + input_device, 'of=' + data_track], return_string), name="'dd' rip")
    dd_process.start()
    sizes = watch_file_size(dd_process, data_track, progress_dict, comm)
    dd_process.join()
    if return_string.value != '':
        return_string.value = "Error: 'dd' failed with non-zero return code"
        return
    progress_dict['total_bytes_extracted'] += progress_dict['current_track_total_bytes']

    track_manifest['tracks'].append({
        'track_number': 1,
        'type': 'data',
        'data_type': cd_toc['tracks'][0]['data_type'],
        'path': data_track,
        'size': os.path.getsize(data_track),
        'sector_size': FORM1_DATA_BYTES_PER_SECTOR,
        'sector_count': int(os.path.getsize(data_track) / FORM1_DATA_BYTES_PER_SECTOR),
        'size_progress': sizes,
        'sha1': checksum_track(data_track)
    })

    # if there are additional tracks, use 'cdparanoia' to extract them
    for track in range(2, cd_toc['track_count']+1):
        progress_dict['current_track_bytes'] = WAV_HEADER_SIZE
        progress_dict['current_track_total_bytes'] = WAV_HEADER_SIZE + cd_toc['tracks'][track-1]['sector_count'] * AUDIO_BYTES_PER_SECTOR
        progress_dict['current_track'] = track
        audio_track = output_directory + '/%02d-audio-track.wav' % (track)
        print([CDPARANOIA, '--quiet', '--output-wav', '--disable-paranoia', '--force-cdrom-device', input_device, str(track), audio_track])
        cdp_process = multiprocessing.Process(target=run_from_other_process, args=([CDPARANOIA, '--quiet', '--output-wav', '--disable-paranoia', '--force-cdrom-device', input_device, str(track), audio_track],), name="'cdparanoia' rip")
        cdp_process.start()
        sizes = watch_file_size(cdp_process, audio_track, progress_dict, comm)
        cdp_process.join()
        if return_string.value != '':
            return_string.value = "'cdparanoia' failed with non-zero return code"
            return
        progress_dict['total_bytes_extracted'] += progress_dict['current_track_total_bytes']

        # add the to the track manifest
        manifest_entry = {
            'track_number': track,
            'type': 'audio',
            'path': audio_track,
            'size': os.path.getsize(audio_track),
            'sector_size': AUDIO_BYTES_PER_SECTOR,
            'sector_count': int((os.path.getsize(audio_track) - WAV_HEADER_SIZE) / AUDIO_BYTES_PER_SECTOR),
            'size_progress': sizes,
            'sha1': checksum_track(data_track)
        }
        track_manifest['tracks'].append(manifest_entry)
    
    # record timing data
    track_manifest['end_time_utc'] = datetime.datetime.utcnow().strftime("%a %b %d %H:%M:%S %Z %Y")
    track_manifest['time_in_seconds'] = int(time.time()) - start_time

    # write a JSON manifest of the rip into the output directory
    manifest_file = open(output_directory + '/track-manifest.json', 'w')
    manifest_file.write(json.dumps(track_manifest, indent=4))
    manifest_file.close()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("USAGE: rip_disc.py <input device> <output directory>")
        sys.exit(1)

    parent_comm, child_comm = multiprocessing.Pipe()
    manager = multiprocessing.Manager()
    return_string = manager.Value(ctypes.c_char_p, '')
    extraction_process = multiprocessing.Process(target=extract_cd, args=(sys.argv[1], sys.argv[2], child_comm, return_string), name="extraction process")
    extraction_process.start()
    while extraction_process.is_alive():
        if parent_comm.poll(1):
            progress_dict = parent_comm.recv()
            print('track %d / %d' % (progress_dict['current_track'], progress_dict['track_count']))
            print('track progress: %d / %d bytes (%d%%)' % (progress_dict['current_track_bytes'], progress_dict['current_track_total_bytes'], 100 * progress_dict['current_track_bytes'] / progress_dict['current_track_total_bytes']))
            print('overall progress: %d / %d bytes (%d%%)' % (progress_dict['total_bytes_extracted'] + progress_dict['current_track_bytes'], progress_dict['total_bytes'], 100 * (progress_dict['total_bytes_extracted'] + progress_dict['current_track_bytes']) / progress_dict['total_bytes']))
    extraction_process.join()
    print(extraction_process.exitcode)
    print("return_string = " + return_string.value)
