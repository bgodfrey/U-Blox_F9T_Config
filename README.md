# U-Blox_F9T_Config

## Introduction
This repo contains scripts for configuring a network of U-Blox F9T receivers intended for differential timing. Specifically, this is intended for the PANOSETI project, however it could be extended to a network of U-Blox receivers. These receivers are interesting for a few reasons

![Sparkfun Zed-F9T](docs/img/Sparkfun_F9T.png 'Sparkfun Zed-F9T receiver')

*U-Blox F9T Receiver available from [Sparkfun](https://web.archive.org/web/20250814191842/https://www.sparkfun.com/sparkfun-gnss-timing-breakout-zed-f9t-qwiic.html)*

1. Multi-band support for bands of interest (GPS, Galileo, and BeiDou) although GLONASS is also supported.
2. Precise timing quoted at 5 ns absolute and 2.5 ns differential 1σ timing accuracy

    a. Sends out quantization (*qerr* corrections that give corrections to the current 1 pps tick)
3. Ability to use one F9T as a base station to send real time correction message (RTCM) correction data to improve accuracy
4. Two time pulse outputs for 1 pps / 10 MHz outputs (configurable up to 25 MHz)
5. Can be operated from a PC's USB port (low power)


PANOSETI is using these as an alternative to more precise fiber-optic based timing systems (e.g. White Rabbit based on Synchronous Ethernet and the IEEE 1588 Precision Time Protocol - see [Wikipedia](https://en.wikipedia.org/wiki/White_Rabbit_Project) for more information) where ultra-precise timing requirements are not required for science and where fiber optic trenching is cost-prohibitive (time-to-science, labor, cost). We use this as a proof-of-concept, *good enough* alternative rather than a de facto replacement.

## Configuration for PANOSETI
PANOSETI consists of a headnode and a bunch of data acquisition (DAQ) nodes. Each telescope/dome has a single DAQ node. Data are collected by each DAQ node and then aggregated onto the headnode at the end of a data acquisition period so as to minimize computational burden on each DAQ node. (This could lead to bottlenecks and ultimately packet loss, which needs to be avoided). This operational principle of minimizing compute on the DAQ nodes has informed the code setup for the GNSS receivers as well.  

![Basic Deisgn Setup](docs/img/F9T_BasicCodeSetup.png 'Basic Design Setup')

*Basic setup of the code where the headnode serves as an intermediary between a base and some number of receivers.*


The picture above describes the basic idea of how the receivers are configured. Each DAQ node can be configured as either a *receiver* or a *base*. Each base pushes RTCM corrections back to the headnode, which is then responsible for forwarding them to any receiver in the network. Designation of base/receiver is decided in a config file (see [Overview of Configuration File](#overview-of-configuration-file)). This config file also contains all the registers needed to configure any client (F9T) in the network. In this way, the network is agnostic to which client is a base/receiver and it can also handle multiple bases/receivers depending on the need of the user. It's also simple to add more devices to the network just be adding to the config file.

## Overview of Setup
This system connects U-Blox F9T GNSS receivers to a central control and data distribution server. It provides configuration, telemetry, and RTCM data streaming over gRPC.

The role of the server is as follows

- Discover all F9T devices on a network
- Configure all F9T devices using a config file
- Receive RTCM messages from a base station and forward them on to any receivers in the network
- Log telemetry data coming from both base stations and receivers (if wanted). This includes data like qerr, UTC timestamps, and information about satellites in view. These data can also be logged locally.
- Ping the devices so the F9T devices know if the headnode can talk with them

Note (as of November 2025) that telemetry includes both the quantization (*qerr*) corrections that are needed for precise differential timing as well as health metrics (e.g. satellites in view). This should probably be separated out since quantization error is required for precise timing while everything falls under quality of life data.


The server exposes two gRPC services:

1. ControlServicer: A bidirectional control plane between each client and the server. This is responsible for sending everything that isn't RTCM data (such as configuration keys, acnowledgements, and ping messages). The different messages types include

    * DeviceHello: Sent by the agent when it first boots up so that the server knows about its existence
    * HelloAck: Sent by the server to the agent - defines the role of the agent as well as what base its associated with and a convenient alias. Right now this alias doubles as the name of the site *(e.g. Gattini) 
    Telemetrty: telemetry data including qerr
    * Ping: Periodically sent by the server to the agent so the agent knows that it can still contact the server. Longterm this should be bi-directional - namely, a ping request by the server elicits a response from the agent. The big picture plan is that this can be used to periodically get metadata that can be plotted / viewed in Grafana.
    * CfgSet: Sent by the server to the agent - configuration keys / settings for each agent
    * ApplyResult: Confirmation from the agent that a configuration key was set
    * File handles (FileBegin, FileChunk, FileEnd): There is currently functionality to upload a file to each receiver. This is a byproduct of an old idea to upload configurations via a file. It isn't being used but in principle could be.

2. CasterServicer: Handles RTCM transport between base and receivers. In this setup base agents stream RTCM frames to the server. The server counts and routes frames to subscribers. The workflow is as follows:
    * Agent connects via Control.Pipe and sends a DeviceHello.
    * Server identifies the device and decides:
        * its role (BASE or RECEIVER),
        * its mount (stream name),
        * and its token (auth) -> not used right now
    * Server responds with a HelloAck message.
    * If the agent is new or outdated, the server pushes a CfgSet message with configuration.  
    * The agent applies config and replies with an ApplyResult.
    * For BASE roles:
        
        * The agent opens a Publish() stream to upload RTCM data.
        * The server acknowledges frames with a single PublishAck once the stream closes.
    
    * For RECEIVER roles:
        * The agent opens a Subscribe() stream to receive RTCM from the mount.
        * The server periodically sends Ping messages over the control channel.
        * If an agent does not respond or goes silent, the server marks it offline and reconfigures when it reconnects.

    * The role of the agent is as follows:
        
        * On startup, the agent opens the bidirectional control stream
        * Sends a DeviceHello identifying itself (model, firmware, unique ID, etc.).
        * Waits for a HelloAck from the server with:

            * Its assigned role
            * Mount stream name
            * Token (for authorization if ever used)
        * If the role is a BASE

            * Launches a publish_loop() that sends RTCM frames from the serial port to Caster.Publish()
        * If the role is a RECEIVER

            * Launches a subscribe_loop() that listens for RTCM messages from the server and writes to the GNSS device
        * When receiving a CfgSet message

            * The agent applies configuration [uses pyubx2's UBXMessage.config_set(])
            * Then replies witha  ControlMsg
        * Set up to either periodically send telemtry over the control stream or write locally to a file, which can later be copied over to the headnode

        * Periodically monitors Ping messages froms the server 
            * If silent for too long, resets the previous config version variable and reconfigures the device on the next reconnect
    
        * On shutdown or if the newtwork is lost
            * It closes the Publish() stream cleanly and waits for a final publish acknowledgement



## Overview of Configuration File
The configuration file is, by default, given in *manifest_f9t.json5*. It defines the configuration settings for all clients via a .json5 file organized in a tiered fashion. Information on registers can be found in either the *[Zed-F9T integration manual](https://content.u-blox.com/sites/default/files/ZED-F9T_IntegrationManual_UBX-21040375.pdf)* or in the *[Zed-F9T interface description](https://content.u-blox.com/sites/default/files/ZED-F9T_InterfaceDescription_%28UBX-18053584%29.pdf?utm_content=UBX-18053584)*. These tiers include

* Global settings: Includes settings common to all devices
    *  *apply_to_layers*: Can be RAM, BBR (battery backed RAM), or flash. Flash is persistent so be careful. You may be able to reset via *UBX-CFG-RST*. If you want to reset the BBR, you can tie the *RST* pin low as described in the [Zed-F9T Hookup Guide](https://learn.sparkfun.com/tutorials/gnss-timing-breakout---zed-f9t-qwiic-hookup-guide/all). BBR lasts for about a day.
    * *verify_layers*: Will perform a check on the configuration keys at the given layer to confirm that configuration keys were correctly applied. Will not stop operations though if things don't work though. This is useful because, otherwise, there is no way to tell if you have configured the device with the requested settings.
    * *CFG_SIGNAL_\* keys*: For Panoseti, these are set as following:
    
        * Constellations that are enabled
            
            1. GPS L1C/A, L2C
            2. Galileo E1B/C, E5b
            3. BeiDou: B1I, B2I

        * Constellations that are disabled
            1. GLONASS L1OF,L2OF
            2. QZSS L1C/A, L2C
            3. SBAS (satellite based augmentation system)
            4. NavIC (not supported by the Zed F9T-00B)

        * Note that by default, all satellite systems are enabled so it is important to select which ones you want

* Role settings

    * A client can be either a base or a receiver. Base clients enable RTCM messages that are sent to the receiver. The suggested messages for a reference base station are given in the Zed F9T-Integration manual and are also given below. All are set to rate 1 on the USB.

        * RTCM 1005 Stationary RTK reference station antenna reference point
        * RTCM 1077 GPS MSM7 (type of RTCM message)
        * RTCM 1088 GLONASS MSM7
        * RTCM 1097 Galileo MSM7
        * RTCM 1127 BeiDou MSM7
        * RTCM 1230 GLONASS code-phase biases
        * RTCM 4072.1 Additional reference station information (4072.0 also enabled)
            This message in particular is critical since it is required for the receiver to start using differential correction data

* Device settings
    * Devices are designated by a unique hardware identifier obtained by polling the *UBX-SEC-UNIQID* register. This gives a unique 5-byte hardware ID for each chip. To get this unique ID, a helpful utility script called *get_unique_id.py* is provided. You can run it via
    ```bash
    python get_unique_id.py
    ```
    This assumes that the device is at */dev/ttyACM0*. If it isn't you can add the location of the device as an optional argument. For example:
    ```bash
    python get_unique_id.py '/dev/ttyACM1'
    ```
    This script will also give you some other information (output from polling *UBX-MON-VER*) about the device including the following:
        
    * uniqid: Unique 5-byte hardware ID of the device (output as hex)
    * model: Model of the device
    * fwver: Firmware version
    * protver: Protocol version
    * hwver: Hardware version
    * extensions: Some subset of the following:
            
        1. ROM BASE: Underlying firmware version in ROM
        2. FWVER: Firmware version (TIM implies Time Sync)
        3. PROTVER: Protocol version 
        4. MOD: Model
        5. Supported major GNSS
        6. Supported augmentation systems
        7. Unknown; my guess is that this is unsupported constellations
    
    * Devices are also designated by an alias, which gives the name of the site. This is nice so have more than a hex string to identify the sites by. 
    * Position is used to configure the position of the device. This actually corresponds to a set of registers, but it's nicer to just input coordinates and have the configuration script set the associated registers. This position can be determined however you like, but PANOSETI uses a separate Zed-F9P to record position to high accuracy.
    * There is a port and baud setting in the manifest right now, which isn't used. You can either have the port be auto-discovered or add it when you start the agent script.
    * Antenna information also isn't used for anything except for notes that can be referenced later. 


## Run Instructions

* Install requirements (assumes that you're using Python 3.9 although it seems to work up through Python 3.14)
```bash
pip install -r  requirements.txt
``` 

* generating gRPC-specific code from Protocol Buffer definition files (*caster_setup.proto*) 
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. caster_setup.proto
```

* For the SERVER - run on the headnode
```bash
python server_v1.py
```

* For the CLIENT - run on each DAQ node
```bash
python agent_v1.py
```

### Run options

#### Server
```bash
optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG       optional path to config file
  --ip IP               IP address to bind to default is 0.0.0.0:50051
  --log-file LOG_FILE   optional file path
  -v VERBOSITY, --verbosity VERBOSITY
                        0=errors, 1=warn, 2=info, 3=debug
```
Notes

* The default log location is `./logging`
* The default telemetry location is `./telem`. Note that in order to load the json objects in each line of the jsonl file, that you will need to use the json5 package.
* The default name is SERVER_{UTC time}
* The default configuration file is `manifest_f9t.json5`. This gets copied into the log directory with the start date/time so you know all your settings for a run.
* You can also get help by typing in `python server_v1.py --help`


#### Agent
```bash
usage: agent_v1.py [-h] [-v VERBOSITY] [--log-file LOG_FILE] [--port PORT]

optional arguments:
optional arguments:
  -h, --help            show this help message and exit
  --cast_addr CAST_ADDR
                        caster service address (publish/subscribe)
  --ctrl_addr CTRL_ADDR
                        control service address (bidirectional)
  --log-file LOG_FILE   optional file path
  --port PORT           optional port useful if multiple devices on a single computer
  -v VERBOSITY, --verbosity VERBOSITY
                        0=errors, 1=warn, 2=info, 3=debug
```

Notes
    
* The default log location is the current working directory
* The default name is {Name of dome}-{last 4 digits of device ID}_{UTC time}
* You can also get help by typing in `python agent_v1.py --help`
* The default bind address of the control and cast service is *0.0.0.0:50051"
* If you don't add a control/cast service address it will set itself to the address of the cast/control service (assuming that it is set)
