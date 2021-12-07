#!/usr/bin/env python
#
###############################################################################
#   HBLink - Copyright (C) 2020 Cortney T. Buffington, N0MJS <n0mjs@me.com>
#   GPS/Data - Copyright (C) 2020 Eric Craw, KF7EEL <kf7eel@qsl.net>
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software Foundation,
#   Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA
###############################################################################

'''
This script uploads a beacon to APRS-IS for the GPS/Data Application itself, cuasing it to appear as an igate
an aprs.fi.
'''

import aprslib
import argparse
import os
import config
import time

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', action='store', dest='CONFIG_FILE', help='/full/path/to/config.file (usually gps_data.cfg)')
cli_args = parser.parse_args()

if not cli_args.CONFIG_FILE:
    cli_args.CONFIG_FILE = os.path.dirname(os.path.abspath(__file__))+'/gps_data.cfg'
CONFIG = config.build_config(cli_args.CONFIG_FILE)

print('''
GPS/Data Application Beacon Script by Eric, KF7EEL. https://github.com/kf7eel/hblink3 \n
This script will upload an APRS position for the GPS/Data Application. This usually causes most APRS-IS clients to see the GPS/Data Application
as an i-gate. \n
Using the following setting to upload beacon.\n
Callsign: ''' + CONFIG['GPS_DATA']['APRS_LOGIN_CALL'] + ''' - Position comment: ''' + CONFIG['GPS_DATA']['IGATE_BEACON_COMMENT'] + '''
Beacon time: ''' + CONFIG['GPS_DATA']['IGATE_BEACON_TIME'] + '''
''')

beacon_packet = CONFIG['GPS_DATA']['APRS_LOGIN_CALL'] + '>APHBL3,TCPIP*:!' + CONFIG['GPS_DATA']['IGATE_LATITUDE'] + str(CONFIG['GPS_DATA']['IGATE_BEACON_ICON'][0]) + CONFIG['GPS_DATA']['IGATE_LONGITUDE'] + str(CONFIG['GPS_DATA']['IGATE_BEACON_ICON'][1]) + '/' + CONFIG['GPS_DATA']['IGATE_BEACON_COMMENT']
#print(beacon_packet)
AIS = aprslib.IS(CONFIG['GPS_DATA']['APRS_LOGIN_CALL'], passwd=CONFIG['GPS_DATA']['APRS_LOGIN_PASSCODE'],host=CONFIG['GPS_DATA']['APRS_SERVER'], port=CONFIG['GPS_DATA']['APRS_PORT'])

while int(CONFIG['GPS_DATA']['IGATE_BEACON_TIME']) > 15:
    
    AIS.connect()
    AIS.sendall(beacon_packet)
    print(beacon_packet)
    AIS.close()
    time.sleep(int(CONFIG['GPS_DATA']['IGATE_BEACON_TIME'])*60)
