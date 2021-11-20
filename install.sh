#!/bin/sh
sudo apt-get install git -y
sudo apt install python3-pip -y
#
cd /home/
sudo cat > /home/requirements.txt <<- "EOF"
bitstring>=3.1.5
bitarray>=0.8.1
Twisted>=16.3.0
dmr_utils3>=0.1.19
configparser>=3.0.0
aprslib>=0.6.42
pynmea2
maidenhead
flask
folium
mysql-connector
resettabletimer>=0.7.0
setproctitle


EOF
##
pip3 install -r requirements.txt
sudo rm requirements.txt
cd /opt/
git clone https://github.com/kf7eel/hbnet.git

cd /opt/hbnet
wget https://github.com/hp3icc/D-APRS/raw/main/gps_data.cfg
wget https://github.com/hp3icc/D-APRS/raw/main/user_settings.txt

sudo cat > /bin/menu-daprs <<- "EOF"
#!/bin/bash
while : ; do
choix=$(whiptail --title "TE1ws-Rev12a Raspbian Proyect HP3ICC Esteban Mackay 73." --menu "Suba o Baje con las flechas del teclado y seleccione el numero de opcion:" 16 65 5 \
1 " Editar igate" \
2 " Iniciar Igate " \
3 " Detener Igate " \
4 " Salir del menu " 3>&1 1>&2 2>&3)

exitstatus=$?
#on recupere ce choix
#exitstatus=$?
if [ $exitstatus = 0 ]; then
    echo "Your chosen option:" $choix
else
    echo "You chose cancel."; break;
fi
# case : action en fonction du choix
case $choix in
1)
nano /opt/hbnet/gps_data.cfg ;;
2)
sudo systemctl stop daprs.service && sudo systemctl start daprs.service && sudo systemctl enable daprs.service ;;
3)
sudo systemctl stop daprs.service && sudo systemctl disable daprs.service ;;
4)
break;
esac
done
exit 0
EOF
#
sudo cat > /lib/systemd/system/daprs.service <<- "EOF"
[Unit]
Description=Data bridge APRS
After=network-online.target syslog.target
Wants=network-online.target

[Service]
StandardOutput=null
WorkingDirectory=/opt/hbnet
RestartSec=3
ExecStart=/usr/bin/python3 /opt/hbnet/gps_data.py
Restart=on-abort

[Install]
WantedBy=multi-user.target


EOF
#
sudo chmod +777 /opt/hbnet/user_settings.txt
sudo chmod +x /opt/hbnet/*.py
sudo chmod +x /bin/menu-daprs
sudo chmod 755 /lib/systemd/system/daprs.service
sudo systemctl daemon-reload

menu-daprs
