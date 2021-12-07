#!/usr/bin/env python
#
###############################################################################
#   Copyright (C) 2016-2019 Cortney T. Buffington, N0MJS <n0mjs@me.com>
#   GPS/Data - Copyright (C) 2020 Eric Craw, KF7EEL <kf7eel@qsl.net>
#   Annotated modifications Copyright (C) 2021 Xavier FRS2013
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
This application, in conjuction with it's rule file (rules.py) will
work like a "conference bridge". This is similar to what most hams think of as a
reflector. You define conference bridges and any system joined to that conference
bridge will both receive traffic from, and send traffic to any other system
joined to the same conference bridge. It does not provide end-to-end connectivity
as each end system must individually be joined to a conference bridge (a name
you create in the configuraiton file) to pass traffic.

This program currently only works with group voice calls.
'''

# Python modules we need
import sys
from bitarray import bitarray
from time import time, strftime
import importlib.util

# Twisted is pretty important, so I keep it separate
from twisted.internet.protocol import Factory, Protocol
from twisted.protocols.basic import NetstringReceiver
from twisted.internet import reactor, task

# Things we import from the main hblink module
from hblink import HBSYSTEM, OPENBRIDGE, systems, hblink_handler, reportFactory, REPORT_OPCODES, mk_aliases
from dmr_utils3.utils import bytes_3, int_id, get_alias
from dmr_utils3 import decode, bptc, const
import config
import log
from const import *

# Stuff for socket reporting
import pickle
# REMOVE LATER from datetime import datetime
# The module needs logging, but handlers, etc. are controlled by the parent
import logging
logger = logging.getLogger(__name__)
import traceback

# Import UNIT time from rules.py
from rules import UNIT_TIME, STATIC_UNIT

# modules from gps_data.py
from bitarray import bitarray
from binascii import b2a_hex as ahex
import re
##from binascii import a2b_hex as bhex
import aprslib
import datetime
from bitarray.util import ba2int as ba2num
from bitarray.util import ba2hex as ba2hx
import codecs
#Needed for working with NMEA
import pynmea2

# Modules for executing commands/scripts
import os
from gps_functions import cmd_list

# Module for maidenhead grids
try:
    import maidenhead as mh
except:
    logger.info('Error importing maidenhead module, make sure it is installed.')
# Module for sending email
try:
    import smtplib
except:
    logger.info('Error importing smtplib module, make sure it is installed.')

#Modules for APRS settings
import ast
from pathlib import Path

# Does anybody read this stuff? There's a PEP somewhere that says I should do this.
__author__     = 'Cortney T. Buffington, N0MJS'
__copyright__  = 'Copyright (c) 2016-2019 Cortney T. Buffington, N0MJS and the K0USY Group'
__credits__    = 'Colin Durbridge, G4EML, Steve Zingman, N4IRS; Mike Zingman, N4IRR; Jonathan Naylor, G4KLX; Hans Barthen, DL5DI; Torsten Shultze, DG1HT'
__license__    = 'GNU GPLv3'
__maintainer__ = 'Cort Buffington, N0MJS'
__email__      = 'n0mjs@me.com'

# Module gobal varaibles

#### from gps_data.py ###
##################################################################################################

# Headers for GPS by model of radio:
# AT-D878 - Compressed UDP
# MD-380 - Unified Data Transport
hdr_type = ''
btf = -1
ssid = ''

# From dmr_utils3, modified to decode entire packet. Works for 1/2 rate coded data. 
def decode_full(_data):
    binlc = bitarray(endian='big')   
    binlc.extend([_data[136],_data[121],_data[106],_data[91], _data[76], _data[61], _data[46], _data[31]])
    binlc.extend([_data[152],_data[137],_data[122],_data[107],_data[92], _data[77], _data[62], _data[47], _data[32], _data[17], _data[2]  ])
    binlc.extend([_data[123],_data[108],_data[93], _data[78], _data[63], _data[48], _data[33], _data[18], _data[3],  _data[184],_data[169]])
    binlc.extend([_data[94], _data[79], _data[64], _data[49], _data[34], _data[19], _data[4],  _data[185],_data[170],_data[155],_data[140]])
    binlc.extend([_data[65], _data[50], _data[35], _data[20], _data[5],  _data[186],_data[171],_data[156],_data[141],_data[126],_data[111]])
    binlc.extend([_data[36], _data[21], _data[6],  _data[187],_data[172],_data[157],_data[142],_data[127],_data[112],_data[97], _data[82] ])
    binlc.extend([_data[7],  _data[188],_data[173],_data[158],_data[143],_data[128],_data[113],_data[98], _data[83]])
    #This is the rest of the Full LC data -- the RS1293 FEC that we don't need
    # This is extremely important for SMS and GPS though.
    binlc.extend([_data[68],_data[53],_data[174],_data[159],_data[144],_data[129],_data[114],_data[99],_data[84],_data[69],_data[54],_data[39]])
    binlc.extend([_data[24],_data[145],_data[130],_data[115],_data[100],_data[85],_data[70],_data[55],_data[40],_data[25],_data[10],_data[191]])
    return binlc
   

n_packet_assembly = 0

packet_assembly = ''

final_packet = ''

#Convert DMR packet to binary from MMDVM packet and remove Slot Type and EMB Sync stuff to allow for BPTC 196,96 decoding
def bptc_decode(_data):
        binary_packet = bitarray(decode.to_bits(_data[20:]))
        del binary_packet[98:166]
        return decode_full(binary_packet)
# Placeholder for future header id
def header_ID(_data):
    hex_hdr = str(ahex(bptc_decode(_data)))
    return hex_hdr[2:6]
    # Work in progress, used to determine data format
##    pass

def aprs_send(packet):
    if aprs_callsign == 'N0CALL':
        logger.info('APRS callsighn set to N0CALL, packet not sent.')
        pass
    else:
        AIS = aprslib.IS(aprs_callsign, passwd=aprs_passcode,host=aprs_server, port=aprs_port)
        AIS.connect()
        AIS.sendall(packet)
        AIS.close()
        logger.info('Packet sent to APRS-IS.')

def dashboard_loc_write(call, lat, lon, time, comment):
    dash_entries = ast.literal_eval(os.popen('cat /tmp/gps_data_user_loc.txt').read())
    dash_entries.insert(0, {'call': call, 'lat': lat, 'lon': lon, 'time':time, 'comment':comment})
# Clear old entries
    list_index = 0
    call_count = 0
    new_dash_entries = []
    for i in dash_entries:
        if i['call'] == call:
            if call_count >= 25:
                pass
            else:
                new_dash_entries.append(i)
            call_count = call_count + 1

        if call != i['call']:
            new_dash_entries.append(i)
            pass
        list_index = list_index + 1
    with open(loc_file, 'w') as user_loc_file:
            user_loc_file.write(str(new_dash_entries[:500]))
            user_loc_file.close()
    logger.info('User location saved for dashboard')
    #logger.info(dash_entries)
    
def dashboard_bb_write(call, dmr_id, time, bulletin):
    #try:
    dash_bb = ast.literal_eval(os.popen('cat ' + bb_file).read())
   # except:
    #    dash_entries = []
    dash_bb.insert(0, {'call': call, 'dmr_id': dmr_id, 'time': time, 'bulletin':bulletin})
    with open(bb_file, 'w') as user_bb_file:
            user_bb_file.write(str(dash_bb[:20]))
            user_bb_file.close()
    logger.info('User bulletin entry saved.')
    #logger.info(dash_bb)

def mailbox_write(call, dmr_id, time, message, recipient):
    #try:
    mail_file = ast.literal_eval(os.popen('cat ' + the_mailbox_file).read())
    mail_file.insert(0, {'call': call, 'dmr_id': dmr_id, 'time': time, 'message':message, 'recipient': recipient})
    with open(the_mailbox_file, 'w') as mailbox_file:
            mailbox_file.write(str(mail_file[:100]))
            mailbox_file.close()
    logger.info('User mail saved.')

def mailbox_delete(dmr_id):
    mail_file = ast.literal_eval(os.popen('cat ' + the_mailbox_file).read())
    call = str(get_alias((dmr_id), subscriber_ids))
    new_data = []
    for message in mail_file:
        if message['recipient'] != call:
            new_data.append(message)
    with open(the_mailbox_file, 'w') as mailbox_file:
            mailbox_file.write(str(new_data[:100]))
            mailbox_file.close()
    logger.info('Mailbox updated. Delete occurred.')


def sos_write(dmr_id, time, message):
    user_settings = ast.literal_eval(os.popen('cat ' + user_settings_file).read())
    try:
        if user_settings[dmr_id][1]['ssid'] == '':
            sos_call = user_settings[dmr_id][0]['call'] + '-' + user_ssid
        else:
            sos_call = user_settings[dmr_id][0]['call'] + '-' + user_settings[dmr_id][1]['ssid']
    except:
        sos_call = str(get_alias((dmr_id), subscriber_ids))
    sos_info = {'call': sos_call, 'dmr_id': dmr_id, 'time': time, 'message':message}
    with open(emergency_sos_file, 'w') as sos_file:
            sos_file.write(str(sos_info))
            sos_file.close()
    logger.info('Saved SOS.')

# Send email via SMTP function
def send_email(to_email, email_subject, email_message):
    global smtp_server
    sender_address = email_sender
    account_password = email_password
    smtp_server = smtplib.SMTP_SSL(smtp_server, int(smtp_port))
    smtp_server.login(sender_address, account_password)
    message = "From: " + aprs_callsign + " D-APRS Gateway\nTo: " + to_email + "\nContent-type: text/html\nSubject: " + email_subject + "\n\n" + '<strong>' + email_subject + '</strong><p>&nbsp;</p><h3>' + email_message + '</h3><p>&nbsp;</p><p>This message was sent to you from a D-APRS gateway operated by <strong>' + aprs_callsign + '</strong>. Do not reply as this gateway is only one way at this time.</p>'
    smtp_server.sendmail(sender_address, to_email, message)
    smtp_server.close()

# Thanks for this forum post for this - https://stackoverflow.com/questions/2579535/convert-dd-decimal-degrees-to-dms-degrees-minutes-seconds-in-python

def decdeg2dms(dd):
   is_positive = dd >= 0
   dd = abs(dd)
   minutes,seconds = divmod(dd*3600,60)
   degrees,minutes = divmod(minutes,60)
   degrees = degrees if is_positive else -degrees
   return (degrees,minutes,seconds)

def user_setting_write(dmr_id, setting, value):
##    try:
    # Open file and load as dict for modification
        with open(user_settings_file, 'r') as f:
##            if f.read() == '{}':
##                user_dict = {}
            user_dict = ast.literal_eval(f.read())
            logger.info('Current settings: ' + str(user_dict))
            if dmr_id not in user_dict:
                user_dict[dmr_id] = [{'call': str(get_alias((dmr_id), subscriber_ids))}, {'ssid': ''}, {'icon': ''}, {'comment': ''}]
            if setting.upper() == 'ICON':
                user_dict[dmr_id][2]['icon'] = value
            if setting.upper() == 'SSID':
                user_dict[dmr_id][1]['ssid'] = value  
            if setting.upper() == 'COM':
                user_comment = user_dict[dmr_id][3]['comment'] = value[0:35]
            if setting.upper() == 'APRS':
                user_dict[dmr_id] = [{'call': str(get_alias((dmr_id), subscriber_ids))}, {'ssid': ''}, {'icon': ''}, {'comment': ''}]
            if setting.upper() == 'PIN':
                try:
                    if user_dict[dmr_id]:
                        user_dict[dmr_id][4]['pin'] = value
                    if not user_dict[dmr_id]:
                        user_dict[dmr_id] = [{'call': str(get_alias((dmr_id), subscriber_ids))}, {'ssid': ''}, {'icon': ''}, {'comment': ''}, {'pin': pin}]
                except:
                    user_dict[dmr_id].append({'pin': value})
            f.close()
            logger.info('Loaded user settings. Preparing to write...')
    # Write modified dict to file
        with open(user_settings_file, 'w') as user_dict_file:
            user_dict_file.write(str(user_dict))
            user_dict_file.close()
            logger.info('User setting saved')
            f.close()
            packet_assembly = ''
            
# Process SMS, do something bases on message

def process_sms(_rf_src, sms):
    if sms == 'ID':
        logger.info(str(get_alias(int_id(from_id), subscriber_ids)) + ' - ' + str(int_id(from_id)))
    elif sms == 'TEST':
        logger.info('It works!')
    elif '@ICON' in sms:
        user_setting_write(int_id(_rf_src), re.sub(' .*|@','',sms), re.sub('@ICON| ','',sms))
    elif '@SSID' in sms:
        user_setting_write(int_id(_rf_src), re.sub(' .*|@','',sms), re.sub('@SSID| ','',sms))
    elif '@COM' in sms:
        user_setting_write(int_id(_rf_src), re.sub(' .*|@','',sms), re.sub('@COM |@COM','',sms))
    elif '@PIN' in sms:
        user_setting_write(int_id(_rf_src), re.sub(' .*|@','',sms), int(re.sub('@PIN |@PIN','',sms)))    
    # Write blank entry to cause APRS receive to look for packets for this station.
    elif '@APRS' in sms:
        user_setting_write(int_id(_rf_src), 'APRS', '')
    elif '@BB' in sms:
        dashboard_bb_write(get_alias(int_id(_rf_src), subscriber_ids), int_id(_rf_src), time(), re.sub('@BB|@BB ','',sms))
    elif '@' and ' E-' in sms:
        email_message = str(re.sub('.*@|.* E-', '', sms))
        to_email = str(re.sub(' E-.*', '', sms))
        email_subject = 'New message from ' + str(get_alias(int_id(_rf_src), subscriber_ids))
        logger.info('Email to: ' + to_email)
        logger.info('Message: ' + email_message)
        try:
            send_email(to_email, email_subject, email_message)
            logger.info('Email sent.')
        except Exception as error_exception:
            logger.info('Failed to send email.')
            logger.info(error_exception)
            logger.info(str(traceback.extract_tb(error_exception.__traceback__)))
    elif '@SOS' in sms or '@NOTICE' in sms:
        sos_write(int_id(_rf_src), time(), sms)
    elif '@REM SOS' == sms:
        os.remove(emergency_sos_file)
        logger.info('Removing SOS')
    elif '@' and 'M-' in sms:
        message = re.sub('^@|.* M-|','',sms)
        recipient = re.sub('@| M-.*','',sms)
        mailbox_write(get_alias(int_id(_rf_src), subscriber_ids), int_id(_rf_src), time(), message, str(recipient).upper())
    elif '@REM MAIL' == sms:
        mailbox_delete(_rf_src)
    elif '@MH' in sms:
        grid_square = re.sub('@MH ', '', sms)
        if len(grid_square) < 6:
            pass
        else:
            lat = decdeg2dms(mh.to_location(grid_square)[0])
            lon = decdeg2dms(mh.to_location(grid_square)[1])
            
            if lon[0] < 0:
                lon_dir = 'W'
            if lon[0] > 0:
                lon_dir = 'E'
            if lat[0] < 0:
                lat_dir = 'S'
            if lat[0] > 0:
                lat_dir = 'N'
            #logger.info(lat)
            #logger.info(lat_dir)
            aprs_lat = str(str(re.sub('\..*|-', '', str(lat[0]))) + str(re.sub('\..*', '', str(lat[1])) + '.')).zfill(5) + '  ' + lat_dir
            aprs_lon = str(str(re.sub('\..*|-', '', str(lon[0]))) + str(re.sub('\..*', '', str(lon[1])) + '.')).zfill(6) + '  ' + lon_dir
        logger.info('Latitude: ' + str(aprs_lat))
        logger.info('Longitude: ' + str(aprs_lon))
        # 14FRS2013 simplified and moved settings retrieval
        user_settings = ast.literal_eval(os.popen('cat ' + user_settings_file).read())	
        if int_id(_rf_src) not in user_settings:	
            ssid = str(user_ssid)	
            icon_table = '/'	
            icon_icon = '['	
            comment = aprs_comment + ' DMR ID: ' + str(int_id(_rf_src)) 	
        else:	
            if user_settings[int_id(_rf_src)][1]['ssid'] == '':	
                ssid = user_ssid	
            if user_settings[int_id(_rf_src)][3]['comment'] == '':	
                comment = aprs_comment + ' DMR ID: ' + str(int_id(_rf_src))	
            if user_settings[int_id(_rf_src)][2]['icon'] == '':	
                icon_table = '/'	
                icon_icon = '['	
            if user_settings[int_id(_rf_src)][2]['icon'] != '':	
                icon_table = user_settings[int_id(_rf_src)][2]['icon'][0]	
                icon_icon = user_settings[int_id(_rf_src)][2]['icon'][1]	
            if user_settings[int_id(_rf_src)][1]['ssid'] != '':	
                ssid = user_settings[int_id(_rf_src)][1]['ssid']	
            if user_settings[int_id(_rf_src)][3]['comment'] != '':	
                comment = user_settings[int_id(_rf_src)][3]['comment']	
        aprs_loc_packet = str(get_alias(int_id(_rf_src), subscriber_ids)) + '-' + ssid + '>APHBL3,TCPIP*:@' + str(datetime.datetime.utcnow().strftime("%H%M%Sh")) + str(aprs_lat) + icon_table + str(aprs_lon) + icon_icon + '/' + str(comment)
        logger.info(aprs_loc_packet)
        logger.info('User comment: ' + comment)
        logger.info('User SSID: ' + ssid)
        logger.info('User icon: ' + icon_table + icon_icon)
        try:
            aprslib.parse(aprs_loc_packet)
            aprs_send(aprs_loc_packet)
            dashboard_loc_write(str(get_alias(int_id(_rf_src), subscriber_ids)) + '-' + ssid, aprs_lat, aprs_lon, time(), comment)
            #logger.info('Sent manual position to APRS')
        except Exception as error_exception:
            logger.info('Exception. Not uploaded')
            logger.info(error_exception)
            logger.info(str(traceback.extract_tb(error_exception.__traceback__)))
        packet_assembly = ''
          
            
    elif 'A-' in sms and '@' in sms:
        #Example SMS text: @ARMDS A-This is a test.
        aprs_dest = re.sub('@| A-.*','',sms)
        aprs_msg = re.sub('^@|.* A-|','',sms)
        logger.info('APRS message to ' + aprs_dest.upper() + '. Message: ' + aprs_msg)
        user_settings = ast.literal_eval(os.popen('cat ' + user_settings_file).read())
        if int_id(_rf_src) in user_settings and user_settings[int_id(_rf_src)][1]['ssid'] != '':
            ssid = user_settings[int_id(_rf_src)][1]['ssid']
        else:
            ssid = user_ssid
        aprs_msg_pkt = str(get_alias(int_id(_rf_src), subscriber_ids)) + '-' + str(ssid) + '>APHBL3,TCPIP*::' + str(aprs_dest).ljust(9).upper() + ':' + aprs_msg[0:73]
        logger.info(aprs_msg_pkt)
        try:
            aprslib.parse(aprs_msg_pkt)
            aprs_send(aprs_msg_pkt)
            #logger.info('Packet sent.')
        except Exception as error_exception:
            logger.info('Error uploading MSG packet.')
            logger.info(error_exception)
            logger.info(str(traceback.extract_tb(error_exception.__traceback__)))
    try:
        if sms in cmd_list:
            logger.info('Executing command/script.')
            os.popen(cmd_list[sms]).read()
            packet_assembly = ''
    except Exception as error_exception:
        logger.info('Exception. Command possibly not in list, or other error.')
        logger.info(error_exception)
        logger.info(str(traceback.extract_tb(error_exception.__traceback__)))
        packet_assembly = ''
    else:
        pass

###########
####

# Dictionary for dynamically mapping unit (subscriber) to a system.
# This is for pruning unit-to-uint calls to not broadcast once the
# target system for a unit is identified
# format 'unit_id': ('SYSTEM', time)
UNIT_MAP = {} 

# UNIX time for end of year 2060. This is used to keep subscribers in UNIT_MAP indefinitely to accomplish static routes for unit calls
#time_2060 = 2871763199.0000000
# 20 years in seconds. added to current at time of start to keep static units from being trimmed.
time_20 = 630720000

# Build a UNIT_MAP based on values in STATIC_MAP.
for i in STATIC_UNIT:
	UNIT_MAP[bytes_3(i[0])] = i[1], time() + time_20

# Timed loop used for reporting HBP status
#
# REPORT BASED ON THE TYPE SELECTED IN THE MAIN CONFIG FILE
def config_reports(_config, _factory):
    if True: #_config['REPORTS']['REPORT']:
        def reporting_loop(logger, _server):
            logger.debug('(REPORT) Periodic reporting loop started')
            _server.send_config()
            _server.send_bridge()

        logger.info('(REPORT) HBlink TCP reporting server configured')

        report_server = _factory(_config)
        report_server.clients = []
        reactor.listenTCP(_config['REPORTS']['REPORT_PORT'], report_server)

        reporting = task.LoopingCall(reporting_loop, logger, report_server)
        reporting.start(_config['REPORTS']['REPORT_INTERVAL'])

    return report_server


# Import Bridging rules
# Note: A stanza *must* exist for any MASTER or CLIENT configured in the main
# configuration file and listed as "active". It can be empty,
# but it has to exist.
def make_bridges(_rules):
    # Convert integer GROUP ID numbers from the config into hex strings
    # we need to send in the actual data packets.
    for _bridge in _rules:
        for _system in _rules[_bridge]:
            if _system['SYSTEM'] not in CONFIG['SYSTEMS']:
                sys.exit('ERROR: Conference bridge "{}" references a system named "{}" that is not enabled in the main configuration'.format(_bridge, _system['SYSTEM']))

            _system['TGID']       = bytes_3(_system['TGID'])
            for i, e in enumerate(_system['ON']):
                _system['ON'][i]  = bytes_3(_system['ON'][i])
            for i, e in enumerate(_system['OFF']):
                _system['OFF'][i] = bytes_3(_system['OFF'][i])
            _system['TIMEOUT']    = _system['TIMEOUT']*60
            if _system['ACTIVE'] == True:
                _system['TIMER']  = time() + _system['TIMEOUT']
            else:
                _system['TIMER']  = time()
    return _rules


# Run this every minute for rule timer updates
def rule_timer_loop():
    global UNIT_MAP
    logger.debug('(ROUTER) routerHBP Rule timer loop started')
    _now = time()

    for _bridge in BRIDGES:
        for _system in BRIDGES[_bridge]:
            if _system['TO_TYPE'] == 'ON':
                if _system['ACTIVE'] == True:
                    if _system['TIMER'] < _now:
                        _system['ACTIVE'] = False
                        logger.info('(ROUTER) Conference Bridge TIMEOUT: DEACTIVATE System: %s, Bridge: %s, TS: %s, TGID: %s', _system['SYSTEM'], _bridge, _system['TS'], int_id(_system['TGID']))
                    else:
                        timeout_in = _system['TIMER'] - _now
                        logger.info('(ROUTER) Conference Bridge ACTIVE (ON timer running): System: %s Bridge: %s, TS: %s, TGID: %s, Timeout in: %.2fs,', _system['SYSTEM'], _bridge, _system['TS'], int_id(_system['TGID']),  timeout_in)
                elif _system['ACTIVE'] == False:
                    logger.debug('(ROUTER) Conference Bridge INACTIVE (no change): System: %s Bridge: %s, TS: %s, TGID: %s', _system['SYSTEM'], _bridge, _system['TS'], int_id(_system['TGID']))
            elif _system['TO_TYPE'] == 'OFF':
                if _system['ACTIVE'] == False:
                    if _system['TIMER'] < _now:
                        _system['ACTIVE'] = True
                        logger.info('(ROUTER) Conference Bridge TIMEOUT: ACTIVATE System: %s, Bridge: %s, TS: %s, TGID: %s', _system['SYSTEM'], _bridge, _system['TS'], int_id(_system['TGID']))
                    else:
                        timeout_in = _system['TIMER'] - _now
                        logger.info('(ROUTER) Conference Bridge INACTIVE (OFF timer running): System: %s Bridge: %s, TS: %s, TGID: %s, Timeout in: %.2fs,', _system['SYSTEM'], _bridge, _system['TS'], int_id(_system['TGID']),  timeout_in)
                elif _system['ACTIVE'] == True:
                    logger.debug('(ROUTER) Conference Bridge ACTIVE (no change): System: %s Bridge: %s, TS: %s, TGID: %s', _system['SYSTEM'], _bridge, _system['TS'], int_id(_system['TGID']))
            else:
                logger.debug('(ROUTER) Conference Bridge NO ACTION: System: %s, Bridge: %s, TS: %s, TGID: %s', _system['SYSTEM'], _bridge, _system['TS'], int_id(_system['TGID']))

    _then = _now - 60 * UNIT_TIME
    remove_list = []
    #logger.info(UNIT_MAP)
    for unit in UNIT_MAP:
       if UNIT_MAP[unit][1] < (_then):
           remove_list.append(unit)

    for unit in remove_list:
        del UNIT_MAP[unit]

    logger.debug('Removed unit(s) %s from UNIT_MAP', remove_list)


    if CONFIG['REPORTS']['REPORT']:
        report_server.send_clients(b'bridge updated')


# run this every 10 seconds to trim orphaned stream ids
def stream_trimmer_loop():
    logger.debug('(ROUTER) Trimming inactive stream IDs from system lists')
    _now = time()

    for system in systems:
        # HBP systems, master and peer
        if CONFIG['SYSTEMS'][system]['MODE'] != 'OPENBRIDGE':
            for slot in range(1,3):
                _slot  = systems[system].STATUS[slot]

                # RX slot check
                if _slot['RX_TYPE'] != HBPF_SLT_VTERM and _slot['RX_TIME'] <  _now - 5:
                    _slot['RX_TYPE'] = HBPF_SLT_VTERM
                    logger.info('(%s) *TIME OUT*  RX STREAM ID: %s SUB: %s TGID %s, TS %s, Duration: %.2f', \
                        system, int_id(_slot['RX_STREAM_ID']), int_id(_slot['RX_RFS']), int_id(_slot['RX_TGID']), slot, _slot['RX_TIME'] - _slot['RX_START'])
                    if CONFIG['REPORTS']['REPORT']:
                        systems[system]._report.send_bridgeEvent('GROUP VOICE,END,RX,{},{},{},{},{},{},{:.2f}'.format(system, int_id(_slot['RX_STREAM_ID']), int_id(_slot['RX_PEER']), int_id(_slot['RX_RFS']), slot, int_id(_slot['RX_TGID']), _slot['RX_TIME'] - _slot['RX_START']).encode(encoding='utf-8', errors='ignore'))

                # TX slot check
                if _slot['TX_TYPE'] != HBPF_SLT_VTERM and _slot['TX_TIME'] <  _now - 5:
                    _slot['TX_TYPE'] = HBPF_SLT_VTERM
                    logger.info('(%s) *TIME OUT*  TX STREAM ID: %s SUB: %s TGID %s, TS %s, Duration: %.2f', \
                        system, int_id(_slot['TX_STREAM_ID']), int_id(_slot['TX_RFS']), int_id(_slot['TX_TGID']), slot, _slot['TX_TIME'] - _slot['TX_START'])
                    if CONFIG['REPORTS']['REPORT']:
                        systems[system]._report.send_bridgeEvent('GROUP VOICE,END,TX,{},{},{},{},{},{},{:.2f}'.format(system, int_id(_slot['TX_STREAM_ID']), int_id(_slot['TX_PEER']), int_id(_slot['TX_RFS']), slot, int_id(_slot['TX_TGID']), _slot['TX_TIME'] - _slot['TX_START']).encode(encoding='utf-8', errors='ignore'))

        # OBP systems
        # We can't delete items from a dicationry that's being iterated, so we have to make a temporarly list of entrys to remove later
        if CONFIG['SYSTEMS'][system]['MODE'] == 'OPENBRIDGE':
            remove_list = []
            for stream_id in systems[system].STATUS:
                if systems[system].STATUS[stream_id]['LAST'] < _now - 5:
                    remove_list.append(stream_id)
            for stream_id in remove_list:
                if stream_id in systems[system].STATUS:
                    _stream = systems[system].STATUS[stream_id]
                    _sysconfig = CONFIG['SYSTEMS'][system]
                    if systems[system].STATUS[stream_id]['ACTIVE']:
                        logger.info('(%s) *TIME OUT*   STREAM ID: %s SUB: %s PEER: %s TYPE: %s DST ID: %s TS 1 Duration: %.2f', \
                        system, int_id(stream_id), get_alias(int_id(_stream['RFS']), subscriber_ids), get_alias(int_id(_sysconfig['NETWORK_ID']), peer_ids), _stream['TYPE'], get_alias(int_id(_stream['DST']), talkgroup_ids), _stream['LAST'] - _stream['START'])
                    if CONFIG['REPORTS']['REPORT']:
                            if _stream['TYPE'] == 'GROUP':
                                systems[system]._report.send_bridgeEvent('GROUP VOICE,END,RX,{},{},{},{},{},{},{:.2f}'.format(system, int_id(stream_id), int_id(_sysconfig['NETWORK_ID']), int_id(_stream['RFS']), 1, int_id(_stream['DST']), _stream['LAST'] - _stream['START']).encode(encoding='utf-8', errors='ignore'))
                            elif _stream['TYPE'] == 'UNIT':
                                systems[system]._report.send_bridgeEvent('UNIT VOICE,END,RX,{},{},{},{},{},{},{:.2f}'.format(system, int_id(stream_id), int_id(_sysconfig['NETWORK_ID']), int_id(_stream['RFS']), 1, int_id(_stream['DST']), _stream['LAST'] - _stream['START']).encode(encoding='utf-8', errors='ignore'))
                    removed = systems[system].STATUS.pop(stream_id)
                else:
                    logger.error('(%s) Attemped to remove OpenBridge Stream ID %s not in the Stream ID list: %s', system, int_id(stream_id), [id for id in systems[system].STATUS])

class routerOBP(OPENBRIDGE):

    def __init__(self, _name, _config, _report):
        OPENBRIDGE.__init__(self, _name, _config, _report)
        self.name = _name
        self.STATUS = {}
        
        # list of self._targets for unit (subscriber, private) calls
        self._targets = []

    def group_received(self, _peer_id, _rf_src, _dst_id, _seq, _slot, _frame_type, _dtype_vseq, _stream_id, _data):
        pkt_time = time()
        dmrpkt = _data[20:53]
        _bits = _data[15]
        
        # Is this a new call stream?
        if (_stream_id not in self.STATUS):
            # This is a new call stream
            self.STATUS[_stream_id] = {
                'START':     pkt_time,
                'CONTENTION':False,
                'RFS':       _rf_src,
                'TYPE':      'GROUP',
                'DST':       _dst_id,
                'ACTIVE':    True
            }

            # If we can, use the LC from the voice header as to keep all options intact
            if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD:
                decoded = decode.voice_head_term(dmrpkt)
                self.STATUS[_stream_id]['LC'] = decoded['LC']

            # If we don't have a voice header then don't wait to decode the Embedded LC
            # just make a new one from the HBP header. This is good enough, and it saves lots of time
            else:
                self.STATUS[_stream_id]['LC'] = LC_OPT + _dst_id + _rf_src


            logger.info('(%s) *GROUP CALL START* OBP STREAM ID: %s SUB: %s (%s) PEER: %s (%s) TGID %s (%s), TS %s', \
                    self._system, int_id(_stream_id), get_alias(_rf_src, subscriber_ids), int_id(_rf_src), get_alias(_peer_id, peer_ids), int_id(_peer_id), get_alias(_dst_id, talkgroup_ids), int_id(_dst_id), _slot)
            if CONFIG['REPORTS']['REPORT']:
                self._report.send_bridgeEvent('GROUP VOICE,START,RX,{},{},{},{},{},{}'.format(self._system, int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _slot, int_id(_dst_id)).encode(encoding='utf-8', errors='ignore'))

        self.STATUS[_stream_id]['LAST'] = pkt_time


        for _bridge in BRIDGES:
            for _system in BRIDGES[_bridge]:

                if (_system['SYSTEM'] == self._system and _system['TGID'] == _dst_id and _system['TS'] == _slot and _system['ACTIVE'] == True):

                    for _target in BRIDGES[_bridge]:
                        if (_target['SYSTEM'] != self._system) and (_target['ACTIVE']):
                            _target_status = systems[_target['SYSTEM']].STATUS
                            _target_system = self._CONFIG['SYSTEMS'][_target['SYSTEM']]
                            if _target_system['MODE'] == 'OPENBRIDGE':
                                # Is this a new call stream on the target?
                                if (_stream_id not in _target_status):
                                    # This is a new call stream on the target
                                    _target_status[_stream_id] = {
                                        'START':     pkt_time,
                                        'CONTENTION':False,
                                        'RFS':       _rf_src,
                                        'TYPE':      'GROUP',
                                        'DST':       _dst_id,
                                        'ACTIVE':    True
                                    }
                                    # Generate LCs (full and EMB) for the TX stream
                                    dst_lc = b''.join([self.STATUS[_stream_id]['LC'][0:3], _target['TGID'], _rf_src])
                                    _target_status[_stream_id]['H_LC'] = bptc.encode_header_lc(dst_lc)
                                    _target_status[_stream_id]['T_LC'] = bptc.encode_terminator_lc(dst_lc)
                                    _target_status[_stream_id]['EMB_LC'] = bptc.encode_emblc(dst_lc)

                                    logger.info('(%s) Conference Bridge: %s, Call Bridged to OBP System: %s TS: %s, TGID: %s', self._system, _bridge, _target['SYSTEM'], _target['TS'], int_id(_target['TGID']))
                                    if CONFIG['REPORTS']['REPORT']:
                                        systems[_target['SYSTEM']]._report.send_bridgeEvent('GROUP VOICE,START,TX,{},{},{},{},{},{}'.format(_target['SYSTEM'], int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _target['TS'], int_id(_target['TGID'])).encode(encoding='utf-8', errors='ignore'))

                                # Record the time of this packet so we can later identify a stale stream
                                _target_status[_stream_id]['LAST'] = pkt_time
                                # Clear the TS bit -- all OpenBridge streams are effectively on TS1
                                _tmp_bits = _bits & ~(1 << 7)

                                # Assemble transmit HBP packet header
                                _tmp_data = b''.join([_data[:8], _target['TGID'], _data[11:15], _tmp_bits.to_bytes(1, 'big'), _data[16:20]])

                                # MUST TEST FOR NEW STREAM AND IF SO, RE-WRITE THE LC FOR THE TARGET
                                # MUST RE-WRITE DESTINATION TGID IF DIFFERENT
                                # if _dst_id != rule['DST_GROUP']:
                                dmrbits = bitarray(endian='big')
                                dmrbits.frombytes(dmrpkt)
                                # Create a voice header packet (FULL LC)
                                if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD:
                                    dmrbits = _target_status[_stream_id]['H_LC'][0:98] + dmrbits[98:166] + _target_status[_stream_id]['H_LC'][98:197]
                                # Create a voice terminator packet (FULL LC)
                                elif _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VTERM:
                                    dmrbits = _target_status[_stream_id]['T_LC'][0:98] + dmrbits[98:166] + _target_status[_stream_id]['T_LC'][98:197]
                                    if CONFIG['REPORTS']['REPORT']:
                                        call_duration = pkt_time - _target_status[_stream_id]['START']
                                        _target_status[_stream_id]['ACTIVE'] = False
                                        systems[_target['SYSTEM']]._report.send_bridgeEvent('GROUP VOICE,END,TX,{},{},{},{},{},{},{:.2f}'.format(_target['SYSTEM'], int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _target['TS'], int_id(_target['TGID']), call_duration).encode(encoding='utf-8', errors='ignore'))              
                                # Create a Burst B-E packet (Embedded LC)
                                elif _dtype_vseq in [1,2,3,4]:
                                    dmrbits = dmrbits[0:116] + _target_status[_stream_id]['EMB_LC'][_dtype_vseq] + dmrbits[148:264]
                                dmrpkt = dmrbits.tobytes()
                                _tmp_data = b''.join([_tmp_data, dmrpkt])

                            else:
                                # BEGIN CONTENTION HANDLING
                                #
                                # The rules for each of the 4 "ifs" below are listed here for readability. The Frame To Send is:
                                #   From a different group than last RX from this HBSystem, but it has been less than Group Hangtime
                                #   From a different group than last TX to this HBSystem, but it has been less than Group Hangtime
                                #   From the same group as the last RX from this HBSystem, but from a different subscriber, and it has been less than stream timeout
                                #   From the same group as the last TX to this HBSystem, but from a different subscriber, and it has been less than stream timeout
                                # The "continue" at the end of each means the next iteration of the for loop that tests for matching rules
                                #
                                if ((_target['TGID'] != _target_status[_target['TS']]['RX_TGID']) and ((pkt_time - _target_status[_target['TS']]['RX_TIME']) < _target_system['GROUP_HANGTIME'])):
                                    if self.STATUS[_stream_id]['CONTENTION'] == False:
                                        self.STATUS[_stream_id]['CONTENTION'] = True
                                        logger.info('(%s) Call not routed to TGID %s, target active or in group hangtime: HBSystem: %s, TS: %s, TGID: %s', self._system, int_id(_target['TGID']), _target['SYSTEM'], _target['TS'], int_id(_target_status[_target['TS']]['RX_TGID']))
                                    continue
                                if ((_target['TGID'] != _target_status[_target['TS']]['TX_TGID']) and ((pkt_time - _target_status[_target['TS']]['TX_TIME']) < _target_system['GROUP_HANGTIME'])):
                                    if self.STATUS[_stream_id]['CONTENTION'] == False:
                                        self.STATUS[_stream_id]['CONTENTION'] = True
                                        logger.info('(%s) Call not routed to TGID%s, target in group hangtime: HBSystem: %s, TS: %s, TGID: %s', self._system, int_id(_target['TGID']), _target['SYSTEM'], _target['TS'], int_id(_target_status[_target['TS']]['TX_TGID']))
                                    continue
                                if (_target['TGID'] == _target_status[_target['TS']]['RX_TGID']) and ((pkt_time - _target_status[_target['TS']]['RX_TIME']) < STREAM_TO):
                                    if self.STATUS[_stream_id]['CONTENTION'] == False:
                                        self.STATUS[_stream_id]['CONTENTION'] = True
                                        logger.info('(%s) Call not routed to TGID%s, matching call already active on target: HBSystem: %s, TS: %s, TGID: %s', self._system, int_id(_target['TGID']), _target['SYSTEM'], _target['TS'], int_id(_target_status[_target['TS']]['RX_TGID']))
                                    continue
                                if (_target['TGID'] == _target_status[_target['TS']]['TX_TGID']) and (_rf_src != _target_status[_target['TS']]['TX_RFS']) and ((pkt_time - _target_status[_target['TS']]['TX_TIME']) < STREAM_TO):
                                    if self.STATUS[_stream_id]['CONTENTION'] == False:
                                        self.STATUS[_stream_id]['CONTENTION'] = True
                                        logger.info('(%s) Call not routed for subscriber %s, call route in progress on target: HBSystem: %s, TS: %s, TGID: %s, SUB: %s', self._system, int_id(_rf_src), _target['SYSTEM'], _target['TS'], int_id(_target_status[_target['TS']]['TX_TGID']), int_id(_target_status[_target['TS']]['TX_RFS']))
                                    continue

                                # Is this a new call stream?
                                if (_target_status[_target['TS']]['TX_STREAM_ID'] != _stream_id):
                                    # Record the DST TGID and Stream ID
                                    _target_status[_target['TS']]['TX_START'] = pkt_time
                                    _target_status[_target['TS']]['TX_TGID'] = _target['TGID']
                                    _target_status[_target['TS']]['TX_STREAM_ID'] = _stream_id
                                    _target_status[_target['TS']]['TX_RFS'] = _rf_src
                                    _target_status[_target['TS']]['TX_PEER'] = _peer_id
                                    # Generate LCs (full and EMB) for the TX stream
                                    dst_lc = b''.join([self.STATUS[_stream_id]['LC'][0:3], _target['TGID'], _rf_src])
                                    _target_status[_target['TS']]['TX_H_LC'] = bptc.encode_header_lc(dst_lc)
                                    _target_status[_target['TS']]['TX_T_LC'] = bptc.encode_terminator_lc(dst_lc)
                                    _target_status[_target['TS']]['TX_EMB_LC'] = bptc.encode_emblc(dst_lc)
                                    logger.debug('(%s) Generating TX FULL and EMB LCs for HomeBrew destination: System: %s, TS: %s, TGID: %s', self._system, _target['SYSTEM'], _target['TS'], int_id(_target['TGID']))
                                    logger.info('(%s) Conference Bridge: %s, Call Bridged to HBP System: %s TS: %s, TGID: %s', self._system, _bridge, _target['SYSTEM'], _target['TS'], int_id(_target['TGID']))
                                    if CONFIG['REPORTS']['REPORT']:
                                       systems[_target['SYSTEM']]._report.send_bridgeEvent('GROUP VOICE,START,TX,{},{},{},{},{},{}'.format(_target['SYSTEM'], int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _target['TS'], int_id(_target['TGID'])).encode(encoding='utf-8', errors='ignore'))

                                # Set other values for the contention handler to test next time there is a frame to forward
                                _target_status[_target['TS']]['TX_TIME'] = pkt_time
                                _target_status[_target['TS']]['TX_TYPE'] = _dtype_vseq

                                # Handle any necessary re-writes for the destination
                                if _system['TS'] != _target['TS']:
                                    _tmp_bits = _bits ^ 1 << 7
                                else:
                                    _tmp_bits = _bits

                                # Assemble transmit HBP packet header
                                _tmp_data = b''.join([_data[:8], _target['TGID'], _data[11:15], _tmp_bits.to_bytes(1, 'big'), _data[16:20]])

                                # MUST TEST FOR NEW STREAM AND IF SO, RE-WRITE THE LC FOR THE TARGET
                                # MUST RE-WRITE DESTINATION TGID IF DIFFERENT
                                # if _dst_id != rule['DST_GROUP']:
                                dmrbits = bitarray(endian='big')
                                dmrbits.frombytes(dmrpkt)
                                # Create a voice header packet (FULL LC)
                                if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD:
                                    dmrbits = _target_status[_target['TS']]['TX_H_LC'][0:98] + dmrbits[98:166] + _target_status[_target['TS']]['TX_H_LC'][98:197]
                                # Create a voice terminator packet (FULL LC)
                                elif _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VTERM:
                                    dmrbits = _target_status[_target['TS']]['TX_T_LC'][0:98] + dmrbits[98:166] + _target_status[_target['TS']]['TX_T_LC'][98:197]
                                    if CONFIG['REPORTS']['REPORT']:
                                        call_duration = pkt_time - _target_status[_target['TS']]['TX_START']
                                        systems[_target['SYSTEM']]._report.send_bridgeEvent('GROUP VOICE,END,TX,{},{},{},{},{},{},{:.2f}'.format(_target['SYSTEM'], int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _target['TS'], int_id(_target['TGID']), call_duration).encode(encoding='utf-8', errors='ignore'))
                                # Create a Burst B-E packet (Embedded LC)
                                elif _dtype_vseq in [1,2,3,4]:
                                    dmrbits = dmrbits[0:116] + _target_status[_target['TS']]['TX_EMB_LC'][_dtype_vseq] + dmrbits[148:264]
                                dmrpkt = dmrbits.tobytes()
                                _tmp_data = b''.join([_tmp_data, dmrpkt, b'\x00\x00']) # Add two bytes of nothing since OBP doesn't include BER & RSSI bytes #_data[53:55]

                            # Transmit the packet to the destination system
                            systems[_target['SYSTEM']].send_system(_tmp_data)
                            #logger.debug('(%s) Packet routed by bridge: %s to system: %s TS: %s, TGID: %s', self._system, _bridge, _target['SYSTEM'], _target['TS'], int_id(_target['TGID']))


        # Final actions - Is this a voice terminator?
        if (_frame_type == HBPF_DATA_SYNC) and (_dtype_vseq == HBPF_SLT_VTERM):
            call_duration = pkt_time - self.STATUS[_stream_id]['START']
            logger.info('(%s) *GROUP CALL END*   STREAM ID: %s SUB: %s (%s) PEER: %s (%s) TGID %s (%s), TS %s, Duration: %.2f', \
                    self._system, int_id(_stream_id), get_alias(_rf_src, subscriber_ids), int_id(_rf_src), get_alias(_peer_id, peer_ids), int_id(_peer_id), get_alias(_dst_id, talkgroup_ids), int_id(_dst_id), _slot, call_duration)
            if CONFIG['REPORTS']['REPORT']:
               self._report.send_bridgeEvent('GROUP VOICE,END,RX,{},{},{},{},{},{},{:.2f}'.format(self._system, int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _slot, int_id(_dst_id), call_duration).encode(encoding='utf-8', errors='ignore'))
            self.STATUS[_stream_id]['ACTIVE'] = False
            logger.debug('(%s) OpenBridge sourced call stream end, remove terminated Stream ID: %s', self._system, int_id(_stream_id))


    def unit_received(self, _peer_id, _rf_src, _dst_id, _seq, _slot, _frame_type, _dtype_vseq, _stream_id, _data):
        global UNIT_MAP
        pkt_time = time()
        dmrpkt = _data[20:53]
        _bits = _data[15]

        # Check if subscriber is in STATIC_UNIT
        for i in STATIC_UNIT:
            # Subscriber is static. Add 20 years of time.
            if i[0] == int_id(_rf_src):
                map_time = pkt_time + time_20
                logger.debug('Static Unit, update time.')
            # Proceed as normal
            else:
                map_time = pkt_time        
        # Make/update this unit in the UNIT_MAP cache
        UNIT_MAP[_rf_src] = (self.name, map_time)
        
        
        # Is this a new call stream?
        if (_stream_id not in self.STATUS):
            # This is a new call stream
            self.STATUS[_stream_id] = {
                'START':     pkt_time,
                'CONTENTION':False,
                'RFS':       _rf_src,
                'TYPE':      'UNIT',
                'DST':       _dst_id,
                'ACTIVE':    True
            }
                
            # Create a destination list for the call:                
            if _dst_id in UNIT_MAP:
                if UNIT_MAP[_dst_id][0] != self._system:
                    self._targets = [UNIT_MAP[_dst_id][0]]
                else:
                    self._targets = []
                    logger.error('UNIT call to a subscriber on the same system, send nothing')
            else:
                self._targets = list(UNIT)
                self._targets.remove(self._system)
            
            
            # This is a new call stream, so log & report
            logger.info('(%s) *UNIT CALL START* STREAM ID: %s SUB: %s (%s) PEER: %s (%s) UNIT: %s (%s), TS: %s, FORWARD: %s', \
                    self._system, int_id(_stream_id), get_alias(_rf_src, subscriber_ids), int_id(_rf_src), get_alias(_peer_id, peer_ids), int_id(_peer_id), get_alias(_dst_id, talkgroup_ids), int_id(_dst_id), _slot, self._targets)
            if CONFIG['REPORTS']['REPORT']:
                self._report.send_bridgeEvent('UNIT VOICE,START,RX,{},{},{},{},{},{},{}'.format(self._system, int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _slot, int_id(_dst_id), self._targets).encode(encoding='utf-8', errors='ignore'))

        # Record the time of this packet so we can later identify a stale stream
        self.STATUS[_stream_id]['LAST'] = pkt_time

        for _target in self._targets:
            _target_status = systems[_target].STATUS
            _target_system = self._CONFIG['SYSTEMS'][_target]
            
            if self._CONFIG['SYSTEMS'][_target]['MODE'] == 'OPENBRIDGE':
                if (_stream_id not in _target_status):
                    # This is a new call stream on the target
                    _target_status[_stream_id] = {
                        'START':     pkt_time,
                        'CONTENTION':False,
                        'RFS':       _rf_src,
                        'TYPE':      'UNIT',
                        'DST':      _dst_id,
                        'ACTIVE':   True
                    }

                    logger.info('(%s) Unit call bridged to OBP System: %s TS: %s, TGID: %s', self._system, _target, _slot if _target_system['BOTH_SLOTS'] else 1, int_id(_dst_id))
                    if CONFIG['REPORTS']['REPORT']:
                        systems[_target]._report.send_bridgeEvent('UNIT VOICE,START,TX,{},{},{},{},{},{}'.format(_target, int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _slot, int_id(_dst_id)).encode(encoding='utf-8', errors='ignore'))

                # Record the time of this packet so we can later identify a stale stream
                _target_status[_stream_id]['LAST'] = pkt_time
                # Clear the TS bit and follow propper OBP definition, unless "BOTH_SLOTS" is set. This only works for unit calls.
                if _target_system['BOTH_SLOTS']:
                    _tmp_bits = _bits
                else:
                    _tmp_bits = _bits & ~(1 << 7)

                # Assemble transmit HBP packet
                _tmp_data = b''.join([_data[:15], _tmp_bits.to_bytes(1, 'big'), _data[16:20]])
                _data = b''.join([_tmp_data, dmrpkt])
                
                if (_frame_type == HBPF_DATA_SYNC) and (_dtype_vseq == HBPF_SLT_VTERM):
                    _target_status[_stream_id]['ACTIVE'] = False

            else:
                # BEGIN STANDARD CONTENTION HANDLING
                #
                # The rules for each of the 4 "ifs" below are listed here for readability. The Frame To Send is:
                #   From a different group than last RX from this HBSystem, but it has been less than Group Hangtime
                #   From a different group than last TX to this HBSystem, but it has been less than Group Hangtime
                #   From the same group as the last RX from this HBSystem, but from a different subscriber, and it has been less than stream timeout
                #   From the same group as the last TX to this HBSystem, but from a different subscriber, and it has been less than stream timeout
                # The "continue" at the end of each means the next iteration of the for loop that tests for matching rules
                #
                '''
                if ((_dst_id != _target_status[_slot]['RX_TGID']) and ((pkt_time - _target_status[_slot]['RX_TIME']) < _target_system['GROUP_HANGTIME'])):
                    if self.STATUS[_stream_id]['CONTENTION'] == False:
                        self.STATUS[_stream_id]['CONTENTION'] = True
                        logger.info('(%s) Call not routed to TGID %s, target active or in group hangtime: HBSystem: %s, TS: %s, TGID: %s', self._system, int_id(_dst_id), _target, _slot, int_id(_target_status[_slot]['RX_TGID']))
                    continue
                if ((_dst_id != _target_status[_slot]['TX_TGID']) and ((pkt_time - _target_status[_slot]['TX_TIME']) < _target_system['GROUP_HANGTIME'])):
                    if self.STATUS[_stream_id]['CONTENTION'] == False:
                        self.STATUS[_stream_id]['CONTENTION'] = True
                        logger.info('(%s) Call not routed to TGID%s, target in group hangtime: HBSystem: %s, TS: %s, TGID: %s', self._system, int_id(_dst_id), _target, _slot, int_id(_target_status[_slot]['TX_TGID']))
                    continue
                '''
                if (_dst_id == _target_status[_slot]['RX_TGID']) and ((pkt_time - _target_status[_slot]['RX_TIME']) < STREAM_TO):
                    if self.STATUS[_stream_id]['CONTENTION'] == False:
                        self.STATUS[_stream_id]['CONTENTION'] = True
                        logger.info('(%s) Call not routed to TGID%s, matching call already active on target: HBSystem: %s, TS: %s, TGID: %s', self._system, int_id(_dst_id), _target, _slot, int_id(_target_status[_slot]['RX_TGID']))
                    continue
                if (_dst_id == _target_status[_slot]['TX_TGID']) and (_rf_src != _target_status[_slot]['TX_RFS']) and ((pkt_time - _target_status[_slot]['TX_TIME']) < STREAM_TO):
                    if self.STATUS[_stream_id]['CONTENTION'] == False:
                        self.STATUS[_stream_id]['CONTENTION'] = True
                        logger.info('(%s) Call not routed for subscriber %s, call route in progress on target: HBSystem: %s, TS: %s, TGID: %s, SUB: %s', self._system, int_id(_rf_src), _target, _slot, int_id(_target_status[_slot]['TX_TGID']), int_id(_target_status[_slot]['TX_RFS']))
                    continue

                # Record target information if this is a new call stream?
                if (_stream_id not in self.STATUS):
                    # Record the DST TGID and Stream ID
                    _target_status[_slot]['TX_START'] = pkt_time
                    _target_status[_slot]['TX_TGID'] = _dst_id
                    _target_status[_slot]['TX_STREAM_ID'] = _stream_id
                    _target_status[_slot]['TX_RFS'] = _rf_src
                    _target_status[_slot]['TX_PEER'] = _peer_id
                    
                    logger.info('(%s) Unit call bridged to HBP System: %s TS: %s, UNIT: %s', self._system, _target, _slot, int_id(_dst_id))
                    if CONFIG['REPORTS']['REPORT']:
                       systems[_target]._report.send_bridgeEvent('UNIT VOICE,START,TX,{},{},{},{},{},{}'.format(_target, int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _slot, int_id(_dst_id)).encode(encoding='utf-8', errors='ignore'))

                # Set other values for the contention handler to test next time there is a frame to forward
                _target_status[_slot]['TX_TIME'] = pkt_time
                _target_status[_slot]['TX_TYPE'] = _dtype_vseq

            #send the call:
            systems[_target].send_system(_data)
            
            if _target_system['MODE'] == 'OPENBRIDGE':
                if (_frame_type == HBPF_DATA_SYNC) and (_dtype_vseq == HBPF_SLT_VTERM):
                    if (_stream_id in _target_status):
                        _target_status.pop(_stream_id)

        
        # Final actions - Is this a voice terminator?
        if (_frame_type == HBPF_DATA_SYNC) and (_dtype_vseq == HBPF_SLT_VTERM):
            self._targets = []
            call_duration = pkt_time - self.STATUS[_stream_id]['START']
            logger.info('(%s) *UNIT CALL END*   STREAM ID: %s SUB: %s (%s) PEER: %s (%s) UNIT %s (%s), TS %s, Duration: %.2f', \
                    self._system, int_id(_stream_id), get_alias(_rf_src, subscriber_ids), int_id(_rf_src), get_alias(_peer_id, peer_ids), int_id(_peer_id), get_alias(_dst_id, talkgroup_ids), int_id(_dst_id), _slot, call_duration)
            if CONFIG['REPORTS']['REPORT']:
               self._report.send_bridgeEvent('UNIT VOICE,END,RX,{},{},{},{},{},{},{:.2f}'.format(self._system, int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _slot, int_id(_dst_id), call_duration).encode(encoding='utf-8', errors='ignore'))


    def dmrd_received(self, _peer_id, _rf_src, _dst_id, _seq, _slot, _call_type, _frame_type, _dtype_vseq, _stream_id, _data):

        if _call_type == 'group':
            self.group_received(_peer_id, _rf_src, _dst_id, _seq, _slot, _frame_type, _dtype_vseq, _stream_id, _data)
        elif _call_type == 'unit':
            self.unit_received(_peer_id, _rf_src, _dst_id, _seq, _slot, _frame_type, _dtype_vseq, _stream_id, _data)
        elif _call_type == 'vcsbk':
            # Route CSBK packets to destination TG. Necessary for group data to work with GPS/Data decoder.
            self.group_received(_peer_id, _rf_src, _dst_id, _seq, _slot, _frame_type, _dtype_vseq, _stream_id, _data)
            logger.debug('CSBK recieved, but HBlink does not process them currently. Packets routed to talkgroup.')

        else:
            logger.error('Unknown call type recieved -- not processed')


class routerHBP(HBSYSTEM):

    def __init__(self, _name, _config, _report):
        HBSYSTEM.__init__(self, _name, _config, _report)
        self.name = _name

        # list of self._targets for unit (subscriber, private) calls
        self._targets = []

        # Status information for the system, TS1 & TS2
        # 1 & 2 are "timeslot"
        # In TX_EMB_LC, 2-5 are burst B-E
        self.STATUS = {
            1: {
                'RX_START':     time(),
                'TX_START':     time(),
                'RX_SEQ':       0,
                'RX_RFS':       b'\x00',
                'TX_RFS':       b'\x00',
                'RX_PEER':      b'\x00',
                'TX_PEER':      b'\x00',
                'RX_STREAM_ID': b'\x00',
                'TX_STREAM_ID': b'\x00',
                'RX_TGID':      b'\x00\x00\x00',
                'TX_TGID':      b'\x00\x00\x00',
                'RX_TIME':      time(),
                'TX_TIME':      time(),
                'RX_TYPE':      HBPF_SLT_VTERM,
                'TX_TYPE':      HBPF_SLT_VTERM,
                'RX_LC':        b'\x00',
                'TX_H_LC':      b'\x00',
                'TX_T_LC':      b'\x00',
                'TX_EMB_LC': {
                    1: b'\x00',
                    2: b'\x00',
                    3: b'\x00',
                    4: b'\x00',
                    }
                },
            2: {
                'RX_START':     time(),
                'TX_START':     time(),
                'RX_SEQ':       0,
                'RX_RFS':       b'\x00',
                'TX_RFS':       b'\x00',
                'RX_PEER':      b'\x00',
                'TX_PEER':      b'\x00',
                'RX_STREAM_ID': b'\x00',
                'TX_STREAM_ID': b'\x00',
                'RX_TGID':      b'\x00\x00\x00',
                'TX_TGID':      b'\x00\x00\x00',
                'RX_TIME':      time(),
                'TX_TIME':      time(),
                'RX_TYPE':      HBPF_SLT_VTERM,
                'TX_TYPE':      HBPF_SLT_VTERM,
                'RX_LC':        b'\x00',
                'TX_H_LC':      b'\x00',
                'TX_T_LC':      b'\x00',
                'TX_EMB_LC': {
                    1: b'\x00',
                    2: b'\x00',
                    3: b'\x00',
                    4: b'\x00',
                    }
                }
            }


    def group_received(self, _peer_id, _rf_src, _dst_id, _seq, _slot, _frame_type, _dtype_vseq, _stream_id, _data):
        global UNIT_MAP
        pkt_time = time()
        dmrpkt = _data[20:53]
        _bits = _data[15]
        
        # Make/update an entry in the UNIT_MAP for this subscriber
        UNIT_MAP[_rf_src] = (self.name, pkt_time)

        # Is this a new call stream?
        if (_stream_id != self.STATUS[_slot]['RX_STREAM_ID']):
            if (self.STATUS[_slot]['RX_TYPE'] != HBPF_SLT_VTERM) and (pkt_time < (self.STATUS[_slot]['RX_TIME'] + STREAM_TO)) and (_rf_src != self.STATUS[_slot]['RX_RFS']):
                logger.warning('(%s) Packet received with STREAM ID: %s <FROM> SUB: %s PEER: %s <TO> TGID %s, SLOT %s collided with existing call', self._system, int_id(_stream_id), int_id(_rf_src), int_id(_peer_id), int_id(_dst_id), _slot)
                return

            # This is a new call stream
            self.STATUS[_slot]['RX_START'] = pkt_time
            logger.info('(%s) *GROUP CALL START* STREAM ID: %s SUB: %s (%s) PEER: %s (%s) TGID %s (%s), TS %s', \
                    self._system, int_id(_stream_id), get_alias(_rf_src, subscriber_ids), int_id(_rf_src), get_alias(_peer_id, peer_ids), int_id(_peer_id), get_alias(_dst_id, talkgroup_ids), int_id(_dst_id), _slot)
            if CONFIG['REPORTS']['REPORT']:
                self._report.send_bridgeEvent('GROUP VOICE,START,RX,{},{},{},{},{},{}'.format(self._system, int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _slot, int_id(_dst_id)).encode(encoding='utf-8', errors='ignore'))

            # If we can, use the LC from the voice header as to keep all options intact
            if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD:
                decoded = decode.voice_head_term(dmrpkt)
                self.STATUS[_slot]['RX_LC'] = decoded['LC']

            # If we don't have a voice header then don't wait to decode it from the Embedded LC
            # just make a new one from the HBP header. This is good enough, and it saves lots of time
            else:
                self.STATUS[_slot]['RX_LC'] = LC_OPT + _dst_id + _rf_src

        for _bridge in BRIDGES:
            for _system in BRIDGES[_bridge]:

                if (_system['SYSTEM'] == self._system and _system['TGID'] == _dst_id and _system['TS'] == _slot and _system['ACTIVE'] == True):

                    for _target in BRIDGES[_bridge]:
                        if _target['SYSTEM'] != self._system:
                            if _target['ACTIVE']:
                                _target_status = systems[_target['SYSTEM']].STATUS
                                _target_system = self._CONFIG['SYSTEMS'][_target['SYSTEM']]

                                if _target_system['MODE'] == 'OPENBRIDGE':
                                    # Is this a new call stream on the target?
                                    if (_stream_id not in _target_status):
                                        # This is a new call stream on the target
                                        _target_status[_stream_id] = {
                                            'START':     pkt_time,
                                            'CONTENTION':False,
                                            'RFS':       _rf_src,
                                            'TYPE':     'GROUP',
                                            'DST':      _dst_id,
                                            'ACTIVE':   True,
                                        }
                                        # Generate LCs (full and EMB) for the TX stream
                                        dst_lc = b''.join([self.STATUS[_slot]['RX_LC'][0:3], _target['TGID'], _rf_src])
                                        _target_status[_stream_id]['H_LC'] = bptc.encode_header_lc(dst_lc)
                                        _target_status[_stream_id]['T_LC'] = bptc.encode_terminator_lc(dst_lc)
                                        _target_status[_stream_id]['EMB_LC'] = bptc.encode_emblc(dst_lc)

                                        logger.info('(%s) Conference Bridge: %s, Call Bridged to OBP System: %s TS: %s, TGID: %s', self._system, _bridge, _target['SYSTEM'], _target['TS'], int_id(_target['TGID']))
                                        if CONFIG['REPORTS']['REPORT']:
                                            systems[_target['SYSTEM']]._report.send_bridgeEvent('GROUP VOICE,START,TX,{},{},{},{},{},{}'.format(_target['SYSTEM'], int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _target['TS'], int_id(_target['TGID'])).encode(encoding='utf-8', errors='ignore'))

                                    # Record the time of this packet so we can later identify a stale stream
                                    _target_status[_stream_id]['LAST'] = pkt_time
                                    # Clear the TS bit -- all OpenBridge streams are effectively on TS1
                                    _tmp_bits = _bits & ~(1 << 7)

                                    # Assemble transmit HBP packet header
                                    _tmp_data = b''.join([_data[:8], _target['TGID'], _data[11:15], _tmp_bits.to_bytes(1, 'big'), _data[16:20]])

                                    # MUST TEST FOR NEW STREAM AND IF SO, RE-WRITE THE LC FOR THE TARGET
                                    # MUST RE-WRITE DESTINATION TGID IF DIFFERENT
                                    # if _dst_id != rule['DST_GROUP']:
                                    dmrbits = bitarray(endian='big')
                                    dmrbits.frombytes(dmrpkt)
                                    # Create a voice header packet (FULL LC)
                                    if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD:
                                        dmrbits = _target_status[_stream_id]['H_LC'][0:98] + dmrbits[98:166] + _target_status[_stream_id]['H_LC'][98:197]
                                    # Create a voice terminator packet (FULL LC)
                                    elif _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VTERM:
                                        dmrbits = _target_status[_stream_id]['T_LC'][0:98] + dmrbits[98:166] + _target_status[_stream_id]['T_LC'][98:197]
                                        if CONFIG['REPORTS']['REPORT']:
                                            call_duration = pkt_time - _target_status[_stream_id]['START']
                                            _target_status[_stream_id]['ACTIVE'] = False
                                            systems[_target['SYSTEM']]._report.send_bridgeEvent('GROUP VOICE,END,TX,{},{},{},{},{},{},{:.2f}'.format(_target['SYSTEM'], int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _target['TS'], int_id(_target['TGID']), call_duration).encode(encoding='utf-8', errors='ignore'))
                                    # Create a Burst B-E packet (Embedded LC)
                                    elif _dtype_vseq in [1,2,3,4]:
                                        dmrbits = dmrbits[0:116] + _target_status[_stream_id]['EMB_LC'][_dtype_vseq] + dmrbits[148:264]
                                    dmrpkt = dmrbits.tobytes()
                                    _tmp_data = b''.join([_tmp_data, dmrpkt])

                                else:
                                    # BEGIN STANDARD CONTENTION HANDLING
                                    #
                                    # The rules for each of the 4 "ifs" below are listed here for readability. The Frame To Send is:
                                    #   From a different group than last RX from this HBSystem, but it has been less than Group Hangtime
                                    #   From a different group than last TX to this HBSystem, but it has been less than Group Hangtime
                                    #   From the same group as the last RX from this HBSystem, but from a different subscriber, and it has been less than stream timeout
                                    #   From the same group as the last TX to this HBSystem, but from a different subscriber, and it has been less than stream timeout
                                    # The "continue" at the end of each means the next iteration of the for loop that tests for matching rules
                                    #
                                    if ((_target['TGID'] != _target_status[_target['TS']]['RX_TGID']) and ((pkt_time - _target_status[_target['TS']]['RX_TIME']) < _target_system['GROUP_HANGTIME'])):
                                        if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD and self.STATUS[_slot]['RX_STREAM_ID'] != _stream_id:
                                            logger.info('(%s) Call not routed to TGID %s, target active or in group hangtime: HBSystem: %s, TS: %s, TGID: %s', self._system, int_id(_target['TGID']), _target['SYSTEM'], _target['TS'], int_id(_target_status[_target['TS']]['RX_TGID']))
                                        continue
                                    if ((_target['TGID'] != _target_status[_target['TS']]['TX_TGID']) and ((pkt_time - _target_status[_target['TS']]['TX_TIME']) < _target_system['GROUP_HANGTIME'])):
                                        if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD and self.STATUS[_slot]['RX_STREAM_ID'] != _stream_id:
                                            logger.info('(%s) Call not routed to TGID%s, target in group hangtime: HBSystem: %s, TS: %s, TGID: %s', self._system, int_id(_target['TGID']), _target['SYSTEM'], _target['TS'], int_id(_target_status[_target['TS']]['TX_TGID']))
                                        continue
                                    if (_target['TGID'] == _target_status[_target['TS']]['RX_TGID']) and ((pkt_time - _target_status[_target['TS']]['RX_TIME']) < STREAM_TO):
                                        if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD and self.STATUS[_slot]['RX_STREAM_ID'] != _stream_id:
                                            logger.info('(%s) Call not routed to TGID%s, matching call already active on target: HBSystem: %s, TS: %s, TGID: %s', self._system, int_id(_target['TGID']), _target['SYSTEM'], _target['TS'], int_id(_target_status[_target['TS']]['RX_TGID']))
                                        continue
                                    if (_target['TGID'] == _target_status[_target['TS']]['TX_TGID']) and (_rf_src != _target_status[_target['TS']]['TX_RFS']) and ((pkt_time - _target_status[_target['TS']]['TX_TIME']) < STREAM_TO):
                                        if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD and self.STATUS[_slot]['RX_STREAM_ID'] != _stream_id:
                                            logger.info('(%s) Call not routed for subscriber %s, call route in progress on target: HBSystem: %s, TS: %s, TGID: %s, SUB: %s', self._system, int_id(_rf_src), _target['SYSTEM'], _target['TS'], int_id(_target_status[_target['TS']]['TX_TGID']), int_id(_target_status[_target['TS']]['TX_RFS']))
                                        continue

                                    # Is this a new call stream?
                                    if (_stream_id != self.STATUS[_slot]['RX_STREAM_ID']):
                                        # Record the DST TGID and Stream ID
                                        _target_status[_target['TS']]['TX_START'] = pkt_time
                                        _target_status[_target['TS']]['TX_TGID'] = _target['TGID']
                                        _target_status[_target['TS']]['TX_STREAM_ID'] = _stream_id
                                        _target_status[_target['TS']]['TX_RFS'] = _rf_src
                                        _target_status[_target['TS']]['TX_PEER'] = _peer_id
                                        # Generate LCs (full and EMB) for the TX stream
                                        dst_lc = self.STATUS[_slot]['RX_LC'][0:3] + _target['TGID'] + _rf_src
                                        _target_status[_target['TS']]['TX_H_LC'] = bptc.encode_header_lc(dst_lc)
                                        _target_status[_target['TS']]['TX_T_LC'] = bptc.encode_terminator_lc(dst_lc)
                                        _target_status[_target['TS']]['TX_EMB_LC'] = bptc.encode_emblc(dst_lc)
                                        logger.debug('(%s) Generating TX FULL and EMB LCs for HomeBrew destination: System: %s, TS: %s, TGID: %s', self._system, _target['SYSTEM'], _target['TS'], int_id(_target['TGID']))
                                        logger.info('(%s) Conference Bridge: %s, Call Bridged to HBP System: %s TS: %s, TGID: %s', self._system, _bridge, _target['SYSTEM'], _target['TS'], int_id(_target['TGID']))
                                        if CONFIG['REPORTS']['REPORT']:
                                            systems[_target['SYSTEM']]._report.send_bridgeEvent('GROUP VOICE,START,TX,{},{},{},{},{},{}'.format(_target['SYSTEM'], int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _target['TS'], int_id(_target['TGID'])).encode(encoding='utf-8', errors='ignore'))

                                    # Set other values for the contention handler to test next time there is a frame to forward
                                    _target_status[_target['TS']]['TX_TIME'] = pkt_time
                                    _target_status[_target['TS']]['TX_TYPE'] = _dtype_vseq

                                    # Handle any necessary re-writes for the destination
                                    if _system['TS'] != _target['TS']:
                                        _tmp_bits = _bits ^ 1 << 7
                                    else:
                                        _tmp_bits = _bits

                                    # Assemble transmit HBP packet header
                                    _tmp_data = b''.join([_data[:8], _target['TGID'], _data[11:15], _tmp_bits.to_bytes(1, 'big'), _data[16:20]])

                                    dmrbits = bitarray(endian='big')
                                    dmrbits.frombytes(dmrpkt)
                                    # Create a voice header packet (FULL LC)
                                    if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD:
                                        dmrbits = _target_status[_target['TS']]['TX_H_LC'][0:98] + dmrbits[98:166] + _target_status[_target['TS']]['TX_H_LC'][98:197]
                                    # Create a voice terminator packet (FULL LC)
                                    elif _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VTERM:
                                        dmrbits = _target_status[_target['TS']]['TX_T_LC'][0:98] + dmrbits[98:166] + _target_status[_target['TS']]['TX_T_LC'][98:197]
                                        if CONFIG['REPORTS']['REPORT']:
                                            call_duration = pkt_time - _target_status[_target['TS']]['TX_START']
                                            systems[_target['SYSTEM']]._report.send_bridgeEvent('GROUP VOICE,END,TX,{},{},{},{},{},{},{:.2f}'.format(_target['SYSTEM'], int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _target['TS'], int_id(_target['TGID']), call_duration).encode(encoding='utf-8', errors='ignore'))
                                    # Create a Burst B-E packet (Embedded LC)
                                    elif _dtype_vseq in [1,2,3,4]:
                                        dmrbits = dmrbits[0:116] + _target_status[_target['TS']]['TX_EMB_LC'][_dtype_vseq] + dmrbits[148:264]
                                    dmrpkt = dmrbits.tobytes()
                                    _tmp_data = b''.join([_tmp_data, dmrpkt, _data[53:55]])

                                # Transmit the packet to the destination system
                                systems[_target['SYSTEM']].send_system(_tmp_data)
                                #logger.debug('(%s) Packet routed by bridge: %s to system: %s TS: %s, TGID: %s', self._system, _bridge, _target['SYSTEM'], _target['TS'], int_id(_target['TGID']))
                                
                                if _target_system['MODE'] == 'OPENBRIDGE':
                                    if (_frame_type == HBPF_DATA_SYNC) and (_dtype_vseq == HBPF_SLT_VTERM) and (self.STATUS[_slot]['RX_TYPE'] != HBPF_SLT_VTERM):
                                        if (_stream_id in _target_status):
                                            _target_status.pop(_stream_id)


        # Final actions - Is this a voice terminator?
        if (_frame_type == HBPF_DATA_SYNC) and (_dtype_vseq == HBPF_SLT_VTERM) and (self.STATUS[_slot]['RX_TYPE'] != HBPF_SLT_VTERM):
            call_duration = pkt_time - self.STATUS[_slot]['RX_START']
            logger.info('(%s) *GROUP CALL END*   STREAM ID: %s SUB: %s (%s) PEER: %s (%s) TGID %s (%s), TS %s, Duration: %.2f', \
                    self._system, int_id(_stream_id), get_alias(_rf_src, subscriber_ids), int_id(_rf_src), get_alias(_peer_id, peer_ids), int_id(_peer_id), get_alias(_dst_id, talkgroup_ids), int_id(_dst_id), _slot, call_duration)
            if CONFIG['REPORTS']['REPORT']:
               self._report.send_bridgeEvent('GROUP VOICE,END,RX,{},{},{},{},{},{},{:.2f}'.format(self._system, int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _slot, int_id(_dst_id), call_duration).encode(encoding='utf-8', errors='ignore'))

            #
            # Begin in-band signalling for call end. This has nothign to do with routing traffic directly.
            #

            # Iterate the rules dictionary

            for _bridge in BRIDGES:
                for _system in BRIDGES[_bridge]:
                    if _system['SYSTEM'] == self._system:

                        # TGID matches a rule source, reset its timer
                        if _slot == _system['TS'] and _dst_id == _system['TGID'] and ((_system['TO_TYPE'] == 'ON' and (_system['ACTIVE'] == True)) or (_system['TO_TYPE'] == 'OFF' and _system['ACTIVE'] == False)):
                            _system['TIMER'] = pkt_time + _system['TIMEOUT']
                            logger.info('(%s) Transmission match for Bridge: %s. Reset timeout to %s', self._system, _bridge, _system['TIMER'])

                        # TGID matches an ACTIVATION trigger
                        if (_dst_id in _system['ON'] or _dst_id in _system['RESET']) and _slot == _system['TS']:
                            # Set the matching rule as ACTIVE
                            if _dst_id in _system['ON']:
                                if _system['ACTIVE'] == False:
                                    _system['ACTIVE'] = True
                                    _system['TIMER'] = pkt_time + _system['TIMEOUT']
                                    logger.info('(%s) Bridge: %s, connection changed to state: %s', self._system, _bridge, _system['ACTIVE'])
                                    # Cancel the timer if we've enabled an "OFF" type timeout
                                    if _system['TO_TYPE'] == 'OFF':
                                        _system['TIMER'] = pkt_time
                                        logger.info('(%s) Bridge: %s set to "OFF" with an on timer rule: timeout timer cancelled', self._system, _bridge)
                            # Reset the timer for the rule
                            if _system['ACTIVE'] == True and _system['TO_TYPE'] == 'ON':
                                _system['TIMER'] = pkt_time + _system['TIMEOUT']
                                logger.info('(%s) Bridge: %s, timeout timer reset to: %s', self._system, _bridge, _system['TIMER'] - pkt_time)

                        # TGID matches an DE-ACTIVATION trigger
                        if (_dst_id in _system['OFF']  or _dst_id in _system['RESET']) and _slot == _system['TS']:
                            # Set the matching rule as ACTIVE
                            if _dst_id in _system['OFF']:
                                if _system['ACTIVE'] == True:
                                    _system['ACTIVE'] = False
                                    logger.info('(%s) Bridge: %s, connection changed to state: %s', self._system, _bridge, _system['ACTIVE'])
                                    # Cancel the timer if we've enabled an "ON" type timeout
                                    if _system['TO_TYPE'] == 'ON':
                                        _system['TIMER'] = pkt_time
                                        logger.info('(%s) Bridge: %s set to ON with and "OFF" timer rule: timeout timer cancelled', self._system, _bridge)
                            # Reset the timer for the rule
                            if _system['ACTIVE'] == False and _system['TO_TYPE'] == 'OFF':
                                _system['TIMER'] = pkt_time + _system['TIMEOUT']
                                logger.info('(%s) Bridge: %s, timeout timer reset to: %s', self._system, _bridge, _system['TIMER'] - pkt_time)
                            # Cancel the timer if we've enabled an "ON" type timeout
                            if _system['ACTIVE'] == True and _system['TO_TYPE'] == 'ON' and _dst_group in _system['OFF']:
                                _system['TIMER'] = pkt_time
                                logger.info('(%s) Bridge: %s set to ON with and "OFF" timer rule: timeout timer cancelled', self._system, _bridge)

        #
        # END IN-BAND SIGNALLING
        #
        # Mark status variables for use later
        self.STATUS[_slot]['RX_PEER']      = _peer_id
        self.STATUS[_slot]['RX_SEQ']       = _seq
        self.STATUS[_slot]['RX_RFS']       = _rf_src
        self.STATUS[_slot]['RX_TYPE']      = _dtype_vseq
        self.STATUS[_slot]['RX_TGID']      = _dst_id
        self.STATUS[_slot]['RX_TIME']      = pkt_time
        self.STATUS[_slot]['RX_STREAM_ID'] = _stream_id


    def unit_received(self, _peer_id, _rf_src, _dst_id, _seq, _slot, _frame_type, _dtype_vseq, _stream_id, _data):
        global UNIT_MAP
        pkt_time = time()
        dmrpkt = _data[20:53]
        _bits = _data[15]

        # Check if subscriber is in STATIC_UNIT
        for i in STATIC_UNIT:
            # Subscriber is static. Add 20 years of time.
            if i[0] == int_id(_rf_src):
                map_time = pkt_time + time_20
                logger.debug('Static Unit, update time.')
            # Proceed as normal
            else:
                map_time = pkt_time
 

        # Make/update this unit in the UNIT_MAP cache
        UNIT_MAP[_rf_src] = (self.name, map_time)
        
        
        # Is this a new call stream?
        if (_stream_id != self.STATUS[_slot]['RX_STREAM_ID']):
            
            # Collision in progress, bail out!
            if (self.STATUS[_slot]['RX_TYPE'] != HBPF_SLT_VTERM) and (pkt_time < (self.STATUS[_slot]['RX_TIME'] + STREAM_TO)) and (_rf_src != self.STATUS[_slot]['RX_RFS']):
                logger.warning('(%s) Packet received with STREAM ID: %s <FROM> SUB: %s PEER: %s <TO> UNIT %s, SLOT %s collided with existing call', self._system, int_id(_stream_id), int_id(_rf_src), int_id(_peer_id), int_id(_dst_id), _slot)
                return
                
            # Create a destination list for the call:
            if _dst_id in UNIT_MAP:
                if UNIT_MAP[_dst_id][0] != self._system:
                    self._targets = [UNIT_MAP[_dst_id][0]]
                else:
                    self._targets = []
                    logger.error('UNIT call to a subscriber on the same system, send nothing')
            else:
                self._targets = list(UNIT)
                self._targets.remove(self._system)
            
            # This is a new call stream, so log & report
            self.STATUS[_slot]['RX_START'] = pkt_time
            logger.info('(%s) *UNIT CALL START* STREAM ID: %s SUB: %s (%s) PEER: %s (%s) UNIT: %s (%s), TS: %s, FORWARD: %s', \
                    self._system, int_id(_stream_id), get_alias(_rf_src, subscriber_ids), int_id(_rf_src), get_alias(_peer_id, peer_ids), int_id(_peer_id), get_alias(_dst_id, talkgroup_ids), int_id(_dst_id), _slot, self._targets)
            if CONFIG['REPORTS']['REPORT']:
                self._report.send_bridgeEvent('UNIT VOICE,START,RX,{},{},{},{},{},{},{}'.format(self._system, int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _slot, int_id(_dst_id), self._targets).encode(encoding='utf-8', errors='ignore'))

        for _target in self._targets:
                
            _target_status = systems[_target].STATUS
            _target_system = self._CONFIG['SYSTEMS'][_target]
            
            if self._CONFIG['SYSTEMS'][_target]['MODE'] == 'OPENBRIDGE':
                if (_stream_id not in _target_status):
                    # This is a new call stream on the target
                    _target_status[_stream_id] = {
                        'START':     pkt_time,
                        'CONTENTION':False,
                        'RFS':       _rf_src,
                        'TYPE':      'UNIT',
                        'DST':      _dst_id,
                        'ACTIVE':   True
                    }

                    logger.info('(%s) Unit call bridged to OBP System: %s TS: %s, UNIT: %s', self._system, _target, _slot if _target_system['BOTH_SLOTS'] else 1, int_id(_dst_id))
                    if CONFIG['REPORTS']['REPORT']:
                        systems[_target]._report.send_bridgeEvent('UNIT VOICE,START,TX,{},{},{},{},{},{}'.format(_target, int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _slot, int_id(_dst_id)).encode(encoding='utf-8', errors='ignore'))

                # Record the time of this packet so we can later identify a stale stream
                _target_status[_stream_id]['LAST'] = pkt_time
                # Clear the TS bit and follow propper OBP definition, unless "BOTH_SLOTS" is set. This only works for unit calls.
                if _target_system['BOTH_SLOTS']:
                    _tmp_bits = _bits
                else:
                    _tmp_bits = _bits & ~(1 << 7)

                # Assemble transmit HBP packet
                _tmp_data = b''.join([_data[:15], _tmp_bits.to_bytes(1, 'big'), _data[16:20]])
                _data = b''.join([_tmp_data, dmrpkt])
                
                if (_frame_type == HBPF_DATA_SYNC) and (_dtype_vseq == HBPF_SLT_VTERM):
                    _target_status[_stream_id]['ACTIVE'] = False

            else:
                # BEGIN STANDARD CONTENTION HANDLING
                #
                # The rules for each of the 4 "ifs" below are listed here for readability. The Frame To Send is:
                #   From a different group than last RX from this HBSystem, but it has been less than Group Hangtime
                #   From a different group than last TX to this HBSystem, but it has been less than Group Hangtime
                #   From the same group as the last RX from this HBSystem, but from a different subscriber, and it has been less than stream timeout
                #   From the same group as the last TX to this HBSystem, but from a different subscriber, and it has been less than stream timeout
                # The "continue" at the end of each means the next iteration of the for loop that tests for matching rules
                #
                '''
                if ((_dst_id != _target_status[_slot]['RX_TGID']) and ((pkt_time - _target_status[_slot]['RX_TIME']) < _target_system['GROUP_HANGTIME'])):
                    if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD and self.STATUS[_slot]['RX_STREAM_ID'] != _stream_id:
                        logger.info('(%s) Call not routed to destination %s, target active or in group hangtime: HBSystem: %s, TS: %s, DEST: %s', self._system, int_id(_dst_id), _target, _slot, int_id(_target_status[_slot]['RX_TGID']))
                    continue
                if ((_dst_id != _target_status[_slot]['TX_TGID']) and ((pkt_time - _target_status[_slot]['TX_TIME']) < _target_system['GROUP_HANGTIME'])):
                    if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD and self.STATUS[_slot]['RX_STREAM_ID'] != _stream_id:
                        logger.info('(%s) Call not routed to destination %s, target in group hangtime: HBSystem: %s, TS: %s, DEST: %s', self._system, int_id(_dst_id), _target, _slot, int_id(_target_status[_slot]['TX_TGID']))
                    continue
                '''
                if (_dst_id == _target_status[_slot]['RX_TGID']) and ((pkt_time - _target_status[_slot]['RX_TIME']) < STREAM_TO):
                    if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD and self.STATUS[_slot]['RX_STREAM_ID'] != _stream_id:
                        logger.info('(%s) Call not routed to destination %s, matching call already active on target: HBSystem: %s, TS: %s, DEST: %s', self._system, int_id(_dst_id), _target, _slot, int_id(_target_status[_slot]['RX_TGID']))
                    continue
                if (_dst_id == _target_status[_slot]['TX_TGID']) and (_rf_src != _target_status[_slot]['TX_RFS']) and ((pkt_time - _target_status[_slot]['TX_TIME']) < STREAM_TO):
                    if _frame_type == HBPF_DATA_SYNC and _dtype_vseq == HBPF_SLT_VHEAD and self.STATUS[_slot]['RX_STREAM_ID'] != _stream_id:
                        logger.info('(%s) Call not routed for subscriber %s, call route in progress on target: HBSystem: %s, TS: %s, DEST: %s, SUB: %s', self._system, int_id(_rf_src), _target, _slot, int_id(_target_status[_slot]['TX_TGID']), int_id(_target_status[_slot]['TX_RFS']))
                    continue

                # Record target information if this is a new call stream?
                if (_stream_id != self.STATUS[_slot]['RX_STREAM_ID']):
                    # Record the DST TGID and Stream ID
                    _target_status[_slot]['TX_START'] = pkt_time
                    _target_status[_slot]['TX_TGID'] = _dst_id
                    _target_status[_slot]['TX_STREAM_ID'] = _stream_id
                    _target_status[_slot]['TX_RFS'] = _rf_src
                    _target_status[_slot]['TX_PEER'] = _peer_id
                    
                    logger.info('(%s) Unit call bridged to HBP System: %s TS: %s, UNIT: %s', self._system, _target, _slot, int_id(_dst_id))
                    if CONFIG['REPORTS']['REPORT']:
                       systems[_target]._report.send_bridgeEvent('UNIT VOICE,START,TX,{},{},{},{},{},{}'.format(_target, int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _slot, int_id(_dst_id)).encode(encoding='utf-8', errors='ignore'))

                # Set other values for the contention handler to test next time there is a frame to forward
                _target_status[_slot]['TX_TIME'] = pkt_time
                _target_status[_slot]['TX_TYPE'] = _dtype_vseq

            #send the call:
            systems[_target].send_system(_data)
                        
        
        # Final actions - Is this a voice terminator?
        if (_frame_type == HBPF_DATA_SYNC) and (_dtype_vseq == HBPF_SLT_VTERM) and (self.STATUS[_slot]['RX_TYPE'] != HBPF_SLT_VTERM):
            self._targets = []
            call_duration = pkt_time - self.STATUS[_slot]['RX_START']
            logger.info('(%s) *UNIT CALL END*   STREAM ID: %s SUB: %s (%s) PEER: %s (%s) UNIT %s (%s), TS %s, Duration: %.2f', \
                    self._system, int_id(_stream_id), get_alias(_rf_src, subscriber_ids), int_id(_rf_src), get_alias(_peer_id, peer_ids), int_id(_peer_id), get_alias(_dst_id, talkgroup_ids), int_id(_dst_id), _slot, call_duration)
            if CONFIG['REPORTS']['REPORT']:
               self._report.send_bridgeEvent('UNIT VOICE,END,RX,{},{},{},{},{},{},{:.2f}'.format(self._system, int_id(_stream_id), int_id(_peer_id), int_id(_rf_src), _slot, int_id(_dst_id), call_duration).encode(encoding='utf-8', errors='ignore'))

        # Mark status variables for use later
        self.STATUS[_slot]['RX_PEER']      = _peer_id
        self.STATUS[_slot]['RX_SEQ']       = _seq
        self.STATUS[_slot]['RX_RFS']       = _rf_src
        self.STATUS[_slot]['RX_TYPE']      = _dtype_vseq
        self.STATUS[_slot]['RX_TGID']      = _dst_id
        self.STATUS[_slot]['RX_TIME']      = pkt_time
        self.STATUS[_slot]['RX_STREAM_ID'] = _stream_id

    def echo_received(self, _peer_id, _rf_src, _dst_id, _seq, _slot, _call_type, _frame_type, _dtype_vseq, _stream_id, _data):
        pkt_time = time()
        dmrpkt = _data[20:53]
        _bits = _data[15]
        if _call_type == 'group':
            
            # Is this is a new call stream?
            if (_stream_id != self.STATUS[_slot]['RX_STREAM_ID']):
                self.STATUS['RX_START'] = pkt_time
                logger.info('(%s) *START RECORDING* STREAM ID: %s SUB: %s (%s) REPEATER: %s (%s) TGID %s (%s), TS %s', \
                                  self._system, int_id(_stream_id), get_alias(_rf_src, subscriber_ids), int_id(_rf_src), get_alias(_peer_id, peer_ids), int_id(_peer_id), get_alias(_dst_id, talkgroup_ids), int_id(_dst_id), _slot)
                self.CALL_DATA.append(_data)
                self.STATUS[_slot]['RX_STREAM_ID'] = _stream_id
                return

            # Final actions - Is this a voice terminator?
            if (_frame_type == const.HBPF_DATA_SYNC) and (_dtype_vseq == const.HBPF_SLT_VTERM) and (self.STATUS[_slot]['RX_TYPE'] != const.HBPF_SLT_VTERM) and (self.CALL_DATA):
                call_duration = pkt_time - self.STATUS['RX_START']
                self.CALL_DATA.append(_data)
                logger.info('(%s) *END   RECORDING* STREAM ID: %s', self._system, int_id(_stream_id))
                sleep(2)
                logger.info('(%s) *START  PLAYBACK* STREAM ID: %s SUB: %s (%s) REPEATER: %s (%s) TGID %s (%s), TS %s, Duration: %s', \
                                  self._system, int_id(_stream_id), get_alias(_rf_src, subscriber_ids), int_id(_rf_src), get_alias(_peer_id, peer_ids), int_id(_peer_id), get_alias(_dst_id, talkgroup_ids), int_id(_dst_id), _slot, call_duration)
                for i in self.CALL_DATA:
                    self.send_system(i)
                    #print(i)
                    sleep(0.06)
                self.CALL_DATA = []
                logger.info('(%s) *END    PLAYBACK* STREAM ID: %s', self._system, int_id(_stream_id))

            else:
                if self.CALL_DATA:
                    self.CALL_DATA.append(_data)


            # Mark status variables for use later
            self.STATUS[_slot]['RX_RFS']       = _rf_src
            self.STATUS[_slot]['RX_TYPE']      = _dtype_vseq
            self.STATUS[_slot]['RX_TGID']      = _dst_id
            self.STATUS[_slot]['RX_TIME']      = pkt_time
            self.STATUS[_slot]['RX_STREAM_ID'] = _stream_id



##### DMR data function ####
    def data_received(self, _peer_id, _rf_src, _dst_id, _seq, _slot, _call_type, _frame_type, _dtype_vseq, _stream_id, _data):
        # Capture data headers
        global n_packet_assembly, hdr_type
        #logger.info(_dtype_vseq)
        logger.info(strftime('%H:%M:%S - %m/%d/%y'))
        #logger.info('Special debug for developement:')
        #logger.info(ahex(bptc_decode(_data)))
        #logger.info(hdr_type)
        #logger.info((ba2num(bptc_decode(_data)[8:12])))
        if int_id(_dst_id) == data_id:
            #logger.info(type(_seq))
            if type(_seq) is bytes:
                pckt_seq = int.from_bytes(_seq, 'big')
            else:
                pckt_seq = _seq
            # Try to classify header
            # UDT header has DPF of 0101, which is 5.
            # If 5 is at position 3, then this should be a UDT header for MD-380 type radios.
            # Coordinates are usually in the very next block after the header, we will discard the rest.
            #logger.info(ahex(bptc_decode(_data)[0:10]))
            if _call_type == call_type and header_ID(_data)[3] == '5' and ba2num(bptc_decode(_data)[69:72]) == 0 and ba2num(bptc_decode(_data)[8:12]) == 0 or (_call_type == 'vcsbk' and header_ID(_data)[3] == '5' and ba2num(bptc_decode(_data)[69:72]) == 0 and ba2num(bptc_decode(_data)[8:12]) == 0):
                global udt_block
                logger.info('MD-380 type UDT header detected. Very next packet should be location.')
                hdr_type = '380'
            if _dtype_vseq == 6 and hdr_type == '380' or _dtype_vseq == 'group' and hdr_type == '380':
                udt_block = 1
            if _dtype_vseq == 7 and hdr_type == '380':
                udt_block = udt_block - 1
                if udt_block == 0:
                    logger.info('MD-380 type packet. This should contain the GPS location.')
                    logger.info('Packet: ' + str(ahex(bptc_decode(_data))))
                    if ba2num(bptc_decode(_data)[1:2]) == 1:
                        lat_dir = 'N'
                    if ba2num(bptc_decode(_data)[1:2]) == 0:
                        lat_dir = 'S'
                    if ba2num(bptc_decode(_data)[2:3]) == 1:
                        lon_dir = 'E'
                    if ba2num(bptc_decode(_data)[2:3]) == 0:
                        lon_dir = 'W'
                    lat_deg = ba2num(bptc_decode(_data)[11:18])
                    lon_deg = ba2num(bptc_decode(_data)[38:46])
                    lat_min = ba2num(bptc_decode(_data)[18:24])
                    lon_min = ba2num(bptc_decode(_data)[46:52])
                    lat_min_dec = str(ba2num(bptc_decode(_data)[24:38])).zfill(4)
                    lon_min_dec = str(ba2num(bptc_decode(_data)[52:66])).zfill(4)
                    # Old MD-380 coordinate format, keep here until new is confirmed working.
                    #aprs_lat = str(str(lat_deg) + str(lat_min) + '.' + str(lat_min_dec)[0:2]).zfill(7) + lat_dir
                    #aprs_lon = str(str(lon_deg) + str(lon_min) + '.' + str(lon_min_dec)[0:2]).zfill(8) + lon_dir
                    # Fix for MD-380 by G7HIF
                    aprs_lat = str(str(lat_deg) + str(lat_min).zfill(2) + '.' + str(lat_min_dec)[0:2]).zfill(7) + lat_dir
                    aprs_lon = str(str(lon_deg) + str(lon_min).zfill(2) + '.' + str(lon_min_dec)[0:2]).zfill(8) + lon_dir

                    # Form APRS packet
                    #logger.info(aprs_loc_packet)
                    logger.info('Lat: ' + str(aprs_lat) + ' Lon: ' + str(aprs_lon))
                    # 14FRS2013 simplified and moved settings retrieval
                    user_settings = ast.literal_eval(os.popen('cat ' + user_settings_file).read())
                    if int_id(_rf_src) not in user_settings:	
                        ssid = str(user_ssid)	
                        icon_table = '/'	
                        icon_icon = '['	
                        comment = aprs_comment + ' DMR ID: ' + str(int_id(_rf_src)) 	
                    else:	
                        if user_settings[int_id(_rf_src)][1]['ssid'] == '':	
                            ssid = user_ssid	
                        if user_settings[int_id(_rf_src)][3]['comment'] == '':	
                            comment = aprs_comment + ' DMR ID: ' + str(int_id(_rf_src))	
                        if user_settings[int_id(_rf_src)][2]['icon'] == '':	
                            icon_table = '/'	
                            icon_icon = '['	
                        if user_settings[int_id(_rf_src)][2]['icon'] != '':	
                            icon_table = user_settings[int_id(_rf_src)][2]['icon'][0]	
                            icon_icon = user_settings[int_id(_rf_src)][2]['icon'][1]	
                        if user_settings[int_id(_rf_src)][1]['ssid'] != '':	
                            ssid = user_settings[int_id(_rf_src)][1]['ssid']	
                        if user_settings[int_id(_rf_src)][3]['comment'] != '':	
                            comment = user_settings[int_id(_rf_src)][3]['comment']
                    aprs_loc_packet = str(get_alias(int_id(_rf_src), subscriber_ids)) + '-' + ssid + '>APHBL3,TCPIP*:@' + str(datetime.datetime.utcnow().strftime("%H%M%Sh")) + str(aprs_lat) + icon_table + str(aprs_lon) + icon_icon + '/' + str(comment)
                    logger.info(aprs_loc_packet)
                    logger.info('User comment: ' + comment)
                    logger.info('User SSID: ' + ssid)
                    logger.info('User icon: ' + icon_table + icon_icon)
                    # Attempt to prevent malformed packets from being uploaded.
                    try:
                        aprslib.parse(aprs_loc_packet)
                        float(lat_deg) < 91
                        float(lon_deg) < 121
                        aprs_send(aprs_loc_packet)
                        dashboard_loc_write(str(get_alias(int_id(_rf_src), subscriber_ids)) + '-' + ssid, aprs_lat, aprs_lon, time(), comment)
                        #logger.info('Sent APRS packet')
                    except Exception as error_exception:
                        logger.info('Error. Failed to send packet. Packet may be malformed.')
                        logger.info(error_exception)
                        logger.info(str(traceback.extract_tb(error_exception.__traceback__)))
                    udt_block = 1
                    hdr_type = ''
                else:
                      pass
            #NMEA type packets for Anytone like radios.
            #if _call_type == call_type or (_call_type == 'vcsbk' and pckt_seq > 3): #int.from_bytes(_seq, 'big') > 3 ):
            # 14FRS2013 contributed improved header filtering, KF7EEL added conditions to allow both call types at the same time
            if _call_type == call_type or (_call_type == 'vcsbk' and pckt_seq > 3 and call_type != 'unit') or (_call_type == 'group' and pckt_seq > 3 and call_type != 'unit') or (_call_type == 'group' and pckt_seq > 3 and call_type == 'both') or (_call_type == 'vcsbk' and pckt_seq > 3 and call_type == 'both') or (_call_type == 'unit' and pckt_seq > 3 and call_type == 'both'): #int.from_bytes(_seq, 'big') > 3 ):
                global packet_assembly, btf
                if _dtype_vseq == 6 or _dtype_vseq == 'group':
                    global btf, hdr_start
                    hdr_start = str(header_ID(_data))
                    logger.info('Header from ' + str(get_alias(int_id(_rf_src), subscriber_ids)) + '. DMR ID: ' + str(int_id(_rf_src)))
                    logger.info(ahex(bptc_decode(_data)))
                    logger.info('Blocks to follow: ' + str(ba2num(bptc_decode(_data)[65:72])))
                    btf = ba2num(bptc_decode(_data)[65:72])
                    # Try resetting packet_assembly
                    packet_assembly = ''
                # Data blocks at 1/2 rate, see https://github.com/g4klx/MMDVM/blob/master/DMRDefines.h for data types. _dtype_seq defined here also
                if _dtype_vseq == 7:
                    btf = btf - 1
                    logger.info('Block #: ' + str(btf))
                    #logger.info(_seq)
                    logger.info('Data block from ' + str(get_alias(int_id(_rf_src), subscriber_ids)) + '. DMR ID: ' + str(int_id(_rf_src)) + '. Destination: ' + str(int_id(_dst_id)))
                    logger.info(ahex(bptc_decode(_data)))
                    if _seq == 0:
                        n_packet_assembly = 0
                        packet_assembly = ''
                        
                    #if btf < btf + 1:
                    # 14FRS2013 removed condition, works great!
                    n_packet_assembly = n_packet_assembly + 1
                    packet_assembly = packet_assembly + str(bptc_decode(_data)) #str((decode_full_lc(b_packet)).strip('bitarray('))
                    # Use block 0 as trigger. $GPRMC must also be in string to indicate NMEA.
                    # This triggers the APRS upload
                    if btf == 0:
                        final_packet = str(bitarray(re.sub("\)|\(|bitarray|'", '', packet_assembly)).tobytes().decode('utf-8', 'ignore'))
                        sms_hex = str(ba2hx(bitarray(re.sub("\)|\(|bitarray|'", '', packet_assembly))))
                        sms_hex_string = re.sub("b'|'", '', str(sms_hex))
                        #NMEA GPS sentence
                        if '$GPRMC' in final_packet or '$GNRMC' in final_packet:
                            logger.info(final_packet + '\n')
                            # Eliminate excess bytes based on NMEA type
                            # GPRMC
                            if 'GPRMC' in final_packet:
                                logger.info('GPRMC location')
                                #nmea_parse = re.sub('A\*.*|.*\$', '', str(final_packet))
                                nmea_parse = re.sub('A\*.*|.*\$|\n.*', '', str(final_packet))
                            # GNRMC
                            if 'GNRMC' in final_packet:
                                logger.info('GNRMC location')
                                nmea_parse = re.sub('.*\$|\n.*|V\*.*', '', final_packet)
                            loc = pynmea2.parse(nmea_parse, check=False)
                            logger.info('Latitude: ' + str(loc.lat) + str(loc.lat_dir) + ' Longitude: ' + str(loc.lon) + str(loc.lon_dir) + ' Direction: ' + str(loc.true_course) + ' Speed: ' + str(loc.spd_over_grnd) + '\n')
                            try:
                                # Begin APRS format and upload
                                # Disable opening file for reading to reduce "collision" or reading and writing at same time.
                                # 14FRS2013 simplified and moved settings retrieval
                                user_settings = ast.literal_eval(os.popen('cat ' + user_settings_file).read())	
                                if int_id(_rf_src) not in user_settings:	
                                    ssid = str(user_ssid)	
                                    icon_table = '/'	
                                    icon_icon = '['	
                                    comment = aprs_comment + ' DMR ID: ' + str(int_id(_rf_src)) 	
                                else:	
                                    if user_settings[int_id(_rf_src)][1]['ssid'] == '':	
                                        ssid = user_ssid	
                                    if user_settings[int_id(_rf_src)][3]['comment'] == '':	
                                        comment = aprs_comment + ' DMR ID: ' + str(int_id(_rf_src))	
                                    if user_settings[int_id(_rf_src)][2]['icon'] == '':	
                                        icon_table = '/'	
                                        icon_icon = '['	
                                    if user_settings[int_id(_rf_src)][2]['icon'] != '':	
                                        icon_table = user_settings[int_id(_rf_src)][2]['icon'][0]	
                                        icon_icon = user_settings[int_id(_rf_src)][2]['icon'][1]	
                                    if user_settings[int_id(_rf_src)][1]['ssid'] != '':	
                                        ssid = user_settings[int_id(_rf_src)][1]['ssid']	
                                    if user_settings[int_id(_rf_src)][3]['comment'] != '':	
                                        comment = user_settings[int_id(_rf_src)][3]['comment']	
                                aprs_loc_packet = str(get_alias(int_id(_rf_src), subscriber_ids)) + '-' + ssid + '>APHBL3,TCPIP*:@' + str(datetime.datetime.utcnow().strftime("%H%M%Sh")) + str(loc.lat[0:7]) + str(loc.lat_dir) + icon_table + str(loc.lon[0:8]) + str(loc.lon_dir) + icon_icon + str(round(loc.true_course)).zfill(3) + '/' + str(round(loc.spd_over_grnd)).zfill(3) + '/' + str(comment)
                                logger.info(aprs_loc_packet)
                                logger.info('User comment: ' + comment)
                                logger.info('User SSID: ' + ssid)
                                logger.info('User icon: ' + icon_table + icon_icon)
                            except Exception as error_exception:
                                logger.info('Error or user settings file not found, proceeding with default settings.')
                                aprs_loc_packet = str(get_alias(int_id(_rf_src), subscriber_ids)) + '-' + str(user_ssid) + '>APHBL3,TCPIP*:@' + str(datetime.datetime.utcnow().strftime("%H%M%Sh")) + str(loc.lat[0:7]) + str(loc.lat_dir) + '/' + str(loc.lon[0:8]) + str(loc.lon_dir) + '[' + str(round(loc.true_course)).zfill(3) + '/' + str(round(loc.spd_over_grnd)).zfill(3) + '/' + aprs_comment + ' DMR ID: ' + str(int_id(_rf_src))
                                logger.info(error_exception)
                                logger.info(str(traceback.extract_tb(error_exception.__traceback__)))
                            try:
                            # Try parse of APRS packet. If it fails, it will not upload to APRS-IS
                                aprslib.parse(aprs_loc_packet)
                            # Float values of lat and lon. Anything that is not a number will cause it to fail.
                                float(loc.lat)
                                float(loc.lon)
                                aprs_send(aprs_loc_packet)
                                dashboard_loc_write(str(get_alias(int_id(_rf_src), subscriber_ids)) + '-' + ssid, str(loc.lat[0:7]) + str(loc.lat_dir), str(loc.lon[0:8]) + str(loc.lon_dir), time(), comment)
                            except Exception as error_exception:
                                logger.info('Failed to parse packet. Packet may be deformed. Not uploaded.')
                                logger.info(error_exception)
                                logger.info(str(traceback.extract_tb(error_exception.__traceback__)))
                            #final_packet = ''
                            # Get callsign based on DMR ID
                            # End APRS-IS upload
                        # Assume this is an SMS message
                        elif '$GPRMC' not in final_packet or '$GNRMC' not in final_packet:
                            
####                            # Motorola type SMS header
##                            if '824a' in hdr_start or '024a' in hdr_start:
##                                logger.info('\nMotorola type SMS')
##                                sms = codecs.decode(bytes.fromhex(''.join(sms_hex[74:-8].split('00'))), 'utf-8')
##                                logger.info('\n\n' + 'Received SMS from ' + str(get_alias(int_id(_rf_src), subscriber_ids)) + ', DMR ID: ' + str(int_id(_rf_src)) + ': ' + str(sms) + '\n')
##                                process_sms(_rf_src, sms)
##                                packet_assembly = ''
##                            # ETSI? type SMS header    
##                            elif '0244' in hdr_start or '8244' in hdr_start:
##                                logger.info('ETSI? type SMS')
##                                sms = codecs.decode(bytes.fromhex(''.join(sms_hex[64:-8].split('00'))), 'utf-8')
##                                logger.info('\n\n' + 'Received SMS from ' + str(get_alias(int_id(_rf_src), subscriber_ids)) + ', DMR ID: ' + str(int_id(_rf_src)) + ': ' + str(sms) + '\n')
##                                #logger.info(final_packet)
##                                #logger.info(sms_hex[64:-8])
##                                process_sms(_rf_src, sms)
##                                packet_assembly = ''
####                                
##                            else:
                                logger.info('\nSMS detected. Attempting to parse.')
                                #logger.info(final_packet)
                                logger.info(sms_hex)
##                                logger.info(type(sms_hex))
                                logger.info('Attempting to find command...')
##                                sms = codecs.decode(bytes.fromhex(''.join(sms_hex[:-8].split('00'))), 'utf-8', 'ignore')
                                sms = codecs.decode(bytes.fromhex(''.join(sms_hex_string[:-8].split('00'))), 'utf-8', 'ignore')
                                msg_found = re.sub('.*\n', '', sms)
                                logger.info('\n\n' + 'Received SMS from ' + str(get_alias(int_id(_rf_src), subscriber_ids)) + ', DMR ID: ' + str(int_id(_rf_src)) + ': ' + str(msg_found) + '\n')
                                process_sms(_rf_src, msg_found)
                                #packet_assembly = ''
                                pass
                                #logger.info(bitarray(re.sub("\)|\(|bitarray|'", '', str(bptc_decode(_data)).tobytes().decode('utf-8', 'ignore'))))
                            #logger.info('\n\n' + 'Received SMS from ' + str(get_alias(int_id(_rf_src), subscriber_ids)) + ', DMR ID: ' + str(int_id(_rf_src)) + ': ' + str(sms) + '\n')
                        # Reset the packet assembly to prevent old data from returning.
                        # 14FRS2013 moved variable reset
                        hdr_start = ''
                        n_packet_assembly = 0	
                        packet_assembly = ''	
                        btf = 0
                    #logger.info(_seq)
                    #packet_assembly = '' #logger.info(_dtype_vseq)
                #logger.info(ahex(bptc_decode(_data)).decode('utf-8', 'ignore'))
                #logger.info(bitarray(re.sub("\)|\(|bitarray|'", '', str(bptc_decode(_data)).tobytes().decode('utf-8', 'ignore'))))


######


    def dmrd_received(self, _peer_id, _rf_src, _dst_id, _seq, _slot, _call_type, _frame_type, _dtype_vseq, _stream_id, _data):
        if _call_type == 'group':
            self.group_received(_peer_id, _rf_src, _dst_id, _seq, _slot, _frame_type, _dtype_vseq, _stream_id, _data)
            # If destination ID = to DATA_DMR_ID, process packet
            if int_id(_dst_id) == data_id:
                self.data_received(_peer_id, _rf_src, _dst_id, _seq, _slot, _call_type, _frame_type, _dtype_vseq, _stream_id, _data)
            # If destination ID = ECHO_DMR_ID, send to playback class
##            if int_id(_dst_id) == echo_id:
##                    logger.info('ECHO packet')
##                    self.echo_received(_peer_id, _rf_src, _dst_id, _seq, _slot, _call_type, _frame_type, _dtype_vseq, _stream_id, _data)
        elif _call_type == 'unit':
        # If destination ID = to DATA_DMR_ID, process packet
            if int_id(_dst_id) == data_id:
                    logger.info('btf' + str(btf))
                    self.data_received(_peer_id, _rf_src, _dst_id, _seq, _slot, _call_type, _frame_type, _dtype_vseq, _stream_id, _data)
            if self._system not in UNIT:
                logger.error('(%s) *UNIT CALL NOT FORWARDED* UNIT calling is disabled for this system (INGRESS)', self._system)
            else:
                self.unit_received(_peer_id, _rf_src, _dst_id, _seq, _slot, _frame_type, _dtype_vseq, _stream_id, _data)

        elif _call_type == 'vcsbk':
            # Route CSBK packets to destination TG. Necessary for group data to work with GPS/Data decoder.
            self.group_received(_peer_id, _rf_src, _dst_id, _seq, _slot, _frame_type, _dtype_vseq, _stream_id, _data)
            logger.debug('CSBK recieved, but HBlink does not process them currently. Packets routed to talkgroup.')
            # If destination ID = to DATA_DMR_ID, process packet
            if int_id(_dst_id) == data_id:
                self.data_received(_peer_id, _rf_src, _dst_id, _seq, _slot, _call_type, _frame_type, _dtype_vseq, _stream_id, _data)
        else:
            logger.error('Unknown call type recieved -- not processed')

#
# Socket-based reporting section
#
class bridgeReportFactory(reportFactory):

    def send_bridge(self):
        serialized = pickle.dumps(BRIDGES, protocol=2) #.decode("utf-8", errors='ignore')
        self.send_clients(REPORT_OPCODES['BRIDGE_SND']+serialized)

    def send_bridgeEvent(self, _data):
        if isinstance(_data, str):
            _data = _data.decode('utf-8', error='ignore')
        self.send_clients(REPORT_OPCODES['BRDG_EVENT']+_data)


#************************************************
#      MAIN PROGRAM LOOP STARTS HERE
#************************************************

if __name__ == '__main__':

    import argparse
    import sys
    import os
    import signal

    # Change the current directory to the location of the application
    os.chdir(os.path.dirname(os.path.realpath(sys.argv[0])))

    # CLI argument parser - handles picking up the config file from the command line, and sending a "help" message
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', action='store', dest='CONFIG_FILE', help='/full/path/to/config.file (usually hblink.cfg)')
    parser.add_argument('-r', '--rules', action='store', dest='RULES_FILE', help='/full/path/to/rules.file (usually rules.py)')
    parser.add_argument('-l', '--logging', action='store', dest='LOG_LEVEL', help='Override config file logging level.')
    cli_args = parser.parse_args()

    # Ensure we have a path for the config file, if one wasn't specified, then use the default (top of file)
    if not cli_args.CONFIG_FILE:
        cli_args.CONFIG_FILE = os.path.dirname(os.path.abspath(__file__))+'/hblink.cfg'

    # Call the external routine to build the configuration dictionary
    CONFIG = config.build_config(cli_args.CONFIG_FILE)

    data_id = int(CONFIG['GPS_DATA']['DATA_DMR_ID'])
    #echo_id = int(CONFIG['GPS_DATA']['ECHO_DMR_ID'])

    # Group call or Unit (private) call
    call_type = CONFIG['GPS_DATA']['CALL_TYPE']
    # APRS-IS login information
    aprs_callsign = CONFIG['GPS_DATA']['APRS_LOGIN_CALL']
    aprs_passcode = int(CONFIG['GPS_DATA']['APRS_LOGIN_PASSCODE'])
    aprs_server = CONFIG['GPS_DATA']['APRS_SERVER']
    aprs_port = int(CONFIG['GPS_DATA']['APRS_PORT'])
    user_ssid = CONFIG['GPS_DATA']['USER_APRS_SSID']
    aprs_comment = CONFIG['GPS_DATA']['USER_APRS_COMMENT']
    # EMAIL variables
    email_sender = CONFIG['GPS_DATA']['EMAIL_SENDER']
    email_password = CONFIG['GPS_DATA']['EMAIL_PASSWORD']
    smtp_server = CONFIG['GPS_DATA']['SMTP_SERVER']
    smtp_port = CONFIG['GPS_DATA']['SMTP_PORT']

    # Dashboard files
    bb_file = CONFIG['GPS_DATA']['BULLETIN_BOARD_FILE']
    loc_file = CONFIG['GPS_DATA']['LOCATION_FILE']
    the_mailbox_file = CONFIG['GPS_DATA']['MAILBOX_FILE']
    emergency_sos_file = CONFIG['GPS_DATA']['EMERGENCY_SOS_FILE']

        # Check if user_settings (for APRS settings of users) exists. Creat it if not.
    if Path(user_settings_file).is_file():
        pass
    else:
        Path(user_settings_file).touch()
        with open(user_settings_file, 'w') as user_dict_file:
            user_dict_file.write("{1: [{'call': 'N0CALL'}, {'ssid': ''}, {'icon': ''}, {'comment': ''}]}")
            user_dict_file.close()
    # Check to see if dashboard files exist
    if Path(loc_file).is_file():
        pass
    else:
        Path(loc_file).touch()
        with open(loc_file, 'w') as user_loc_file:
            user_loc_file.write("[]")
            user_loc_file.close()
    if Path(bb_file).is_file():
        pass
    else:
        Path(bb_file).touch()
        with open(bb_file, 'w') as user_bb_file:
            user_bb_file.write("[]")
            user_bb_file.close()
    if Path(the_mailbox_file).is_file():
        pass
    else:
        Path(the_mailbox_file).touch()
        with open(the_mailbox_file, 'w') as user_loc_file:
            user_loc_file.write("[]")
            user_loc_file.close()

    # Ensure we have a path for the rules file, if one wasn't specified, then use the default (top of file)
    if not cli_args.RULES_FILE:
        cli_args.RULES_FILE = os.path.dirname(os.path.abspath(__file__))+'/rules.py'

    # Start the system logger
    if cli_args.LOG_LEVEL:
        CONFIG['LOGGER']['LOG_LEVEL'] = cli_args.LOG_LEVEL
    logger = log.config_logging(CONFIG['LOGGER'])
    logger.info('\n\nCopyright (c) 2013, 2014, 2015, 2016, 2018, 2019, 2020\n\tThe Regents of the K0USY Group. All rights reserved.\n')
    logger.debug('(GLOBAL) Logging system started, anything from here on gets logged')

    # Set up the signal handler
    def sig_handler(_signal, _frame):
        logger.info('(GLOBAL) SHUTDOWN: CONFBRIDGE IS TERMINATING WITH SIGNAL %s', str(_signal))
        hblink_handler(_signal, _frame)
        logger.info('(GLOBAL) SHUTDOWN: ALL SYSTEM HANDLERS EXECUTED - STOPPING REACTOR')
        reactor.stop()

    # Set signal handers so that we can gracefully exit if need be
    for sig in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, sig_handler)

    # Create the name-number mapping dictionaries
    peer_ids, subscriber_ids, talkgroup_ids = mk_aliases(CONFIG)
    
    # Import the ruiles file as a module, and create BRIDGES from it
    spec = importlib.util.spec_from_file_location("module.name", cli_args.RULES_FILE)
    rules_module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(rules_module)
        logger.info('(ROUTER) Routing bridges file found and bridges imported: %s', cli_args.RULES_FILE)
    except (ImportError, FileNotFoundError):
        sys.exit('(ROUTER) TERMINATING: Routing bridges file not found or invalid: {}'.format(cli_args.RULES_FILE))

    # Build the routing rules file
    BRIDGES = make_bridges(rules_module.BRIDGES)
    
    # Get rule parameter for private calls
    UNIT = rules_module.UNIT

    # INITIALIZE THE REPORTING LOOP
    if CONFIG['REPORTS']['REPORT']:
        report_server = config_reports(CONFIG, bridgeReportFactory)
    else:
        report_server = None
        logger.info('(REPORT) TCP Socket reporting not configured')

    # HBlink instance creation
    logger.info('(GLOBAL) HBlink \'bridge.py\' -- SYSTEM STARTING...')
    for system in CONFIG['SYSTEMS']:
        if CONFIG['SYSTEMS'][system]['ENABLED']:
            if CONFIG['SYSTEMS'][system]['MODE'] == 'OPENBRIDGE':
                systems[system] = routerOBP(system, CONFIG, report_server)
            else:
                systems[system] = routerHBP(system, CONFIG, report_server)
            reactor.listenUDP(CONFIG['SYSTEMS'][system]['PORT'], systems[system], interface=CONFIG['SYSTEMS'][system]['IP'])
            logger.debug('(GLOBAL) %s instance created: %s, %s', CONFIG['SYSTEMS'][system]['MODE'], system, systems[system])
    #aprs_upload(CONFIG)

    def loopingErrHandle(failure):
        logger.error('(GLOBAL) STOPPING REACTOR TO AVOID MEMORY LEAK: Unhandled error in timed loop.\n %s', failure)
        reactor.stop()

    # Initialize the rule timer -- this if for user activated stuff
    rule_timer_task = task.LoopingCall(rule_timer_loop)
    rule_timer = rule_timer_task.start(60)
    rule_timer.addErrback(loopingErrHandle)

    # Initialize the stream trimmer
    stream_trimmer_task = task.LoopingCall(stream_trimmer_loop)
    stream_trimmer = stream_trimmer_task.start(5)
    stream_trimmer.addErrback(loopingErrHandle)

    reactor.run()
