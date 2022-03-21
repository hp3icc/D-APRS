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
Settings for web dashboard.
'''

# Title of the Dashboard
dashboard_title = 'HBLink3 D-APRS Dashboard'
# Logo used on dashboard page
logo = 'https://raw.githubusercontent.com/kf7eel/hblink3/gps/HBlink.png'
# Port to run server
dash_port = 8092
# IP to run server on
dash_host = '127.0.0.1'
#Description of dashboard to show on main page
description = '''
Welcome to the ''' + dashboard_title + '''.
'''
# The following will generate a help page for your users.

# Data call type
data_call_type = 'Private Call'
# DMR ID of GPS/Data application
data_call_id = '9099'
# Default APRS ssid
aprs_ssid = '15'

# Gateway contact info displayed on about page.
contact_name = 'your name'
contact_call = 'N0CALL'
contact_email = 'email@example.org'
contact_website = 'https://hbl.ink'

# Time format for display
time_format = '%H:%M:%S - %m/%d/%y'

# Center dashboard map over these coordinates
map_center = (47.00, -120.00)
zoom_level = 7
# List and preview of some map themes at http://leaflet-extras.github.io/leaflet-providers/preview/
# The following are options for map themes and just work, you should use one of these: “OpenStreetMap”, “Stamen” (Terrain, Toner, and Watercolor),
map_theme = 'Stamen Toner'

# RSS feed link, shows in the link section of each RSS item.
rss_link = 'http://localhost:8092'

