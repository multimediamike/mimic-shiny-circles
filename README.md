# mimic-shiny-circles: Overcomplicated system for the simple task of archiving CD-ROMs

mimic-shiny-circles is a method for archiving CD-ROMs. The underlying philosophy of this tool is to use extract the contents of a CD-ROM twice using 2 dfferent optical drives in order to increase confidence that the extracted bits are correct. Then it stores disc metadata in [Amazon Web Services DynamoDB](https://aws.amazon.com/dynamodb/).

## Components
* cdinfo2json.c: A C program tailored for Linux that reads a compact disc's table of contents and outputs pertinent disc information in JSON format.
* rip_cd.py: A Python program which extracts the data from a CD-ROM. It uses the Linux `dd` command to 
* mimic_shiny_circles.py: The core program that leverages `rip_cd.py` in order to extract 2 CD-ROMs at a time. When the program successfully extracts a pair of discs using 2 different optical drives, it uses [AWS SQS](https://aws.amazon.com/sqs/) to log work for later processing.
* process_pair.py: This program requests messages that were logged by `mimic_shiny_circles.py` and verifies that pairs of CD-ROM rips are identical. If they identical, move one copy (along with a metadata file that includes the confirmed SHA-1 checksum) to a final archival location.

## Setup
* Compile `cdinfo2json`: `gcc -Wall cdinfo2json.c -o cdinfo2json`
* Create a Python3 virtualenv: `virtualenv -p python3 py3-flask`
* Activate virtualenv: `source py3-flask/bin/activate`
* Install Python requirements: `pip install -r requirements.txt`
* Configure a YAML file that specifies a pair of optical drives for processing. Use `config-pair.yml.example` as an example.
* Configure a YAML file that specifies the pair processing step. Use `config-post-processing.yml.example` as an example.

## Running
* Activate virtualenv: `source py3-flask/bin/activate`
* Extract a pair of CD-ROMs using 2 different drives: `python mimic_shiny_circles.py config-pair-1.yml`
* Process the pair and log data into DynamoDB: `python process_pair.py config-post-processing.yml`
