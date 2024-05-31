#!/bin/bash
# Nombre del script
SCRIPT_NAME="emq-daprs.sh"

# Registra el inicio en /opt/curl.txt
echo "Inicio: $SCRIPT_NAME" >> /opt/curl.txt
# Verificar si el usuario tiene permisos de root
if [[ $EUID -ne 0 ]]; then
    echo "Este script debe ejecutarse como usuario ROOT"
    exit 1
fi
#####################################
#
cd /opt/
if [ -d "/opt/D-APRS" ];
then
   rm -r /opt/D-APRS/
 #echo "found file"
else
 echo "dir not found"

fi
###############################
git clone https://github.com/hp3icc/D-APRS.git
cd /opt/D-APRS/

sudo cat > /bin/menu-igate <<- "EOF"
#!/bin/bash
while : ; do
choix=$(whiptail --title "D-APRS KF7EEL / Raspbian Proyect HP3ICC Esteban Mackay 73." --menu "Suba o Baje con las flechas del teclado y seleccione el numero de opcion:" 16 65 7 \
1 " Editar igate" \
2 " Iniciar Igate " \
3 " Detener Igate " \
4 " Dashboard on " \
5 " Dashboard off " \
6 " Salir del menu " 3>&1 1>&2 2>&3)

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
nano /opt/D-APRS/gps_data.cfg &&
variable80=$(grep "DASH_PORT:" /opt/D-APRS/gps_data.cfg | grep -Eo '[A.0-9]{1,9}')
if sudo netstat -tuln | grep -q "0.0.0.0:$variable80 "; then
        whiptail --title "Check Port" --msgbox "El puerto $variable80 esta ocupado  , The port $variable80 is busy" 0 50
else

  if sudo systemctl status daprs-board.service |grep "service; enabled;" >/dev/null 2>&1
   then 
   sudo systemctl stop daprs-board.service
   sudo systemctl start daprs-board.service
  fi
fi ;;
2)
sudo systemctl stop daprs.service && sudo systemctl start daprs.service && sudo systemctl enable daprs.service ;;
3)
sudo systemctl stop daprs.service && sudo systemctl disable daprs.service ;;
4)
if sudo systemctl status daprs-board.service |grep "service; enabled;" >/dev/null 2>&1
   then 
   sudo systemctl disable daprs-board.service
fi
if sudo systemctl status daprs-board.service |grep active >/dev/null 2>&1
then 
   sudo systemctl stop daprs-board.service
fi
variable80=$(grep "DASH_PORT:" /opt/D-APRS/gps_data.cfg | grep -Eo '[A.0-9]{1,9}')
if sudo netstat -tuln | grep -q "0.0.0.0:$variable80 "; then
        whiptail --title "Check Port" --msgbox "El puerto $variable80 esta ocupado  , The port $variable80 is busy" 0 50
else

  if ! sudo systemctl status daprs-board.service |grep "service; enabled;" >/dev/null 2>&1
   then 
   sudo systemctl enable daprs-board.service
  fi
  sudo systemctl start daprs-board.service
fi ;;
5)
sudo systemctl stop daprs-board.service && sudo systemctl disable daprs-board.service ;;
6)
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
WorkingDirectory=/opt/D-APRS
RestartSec=3
ExecStart=/usr/bin/python3 /opt/D-APRS/gps_data.py
Restart=on-abort

[Install]
WantedBy=multi-user.target


EOF
#
sudo cat > /lib/systemd/system/daprs-board.service <<- "EOF"
[Unit]
Description=Dashboard D-APRS
After=network-online.target syslog.target
Wants=network-online.target

[Service]
StandardOutput=null
WorkingDirectory=/opt/D-APRS/dashboard
RestartSec=3
ExecStart=/usr/bin/python3 /opt/D-APRS/dashboard/dashboard.py -c /opt/D-APRS/gps_data.cfg
Restart=on-abort

[Install]
WantedBy=multi-user.target


EOF
#
sudo chmod -R 777 /opt/D-APRS/*
sudo chmod -R +x /opt/D-APRS/*
ln -sf /bin/menu-igate /bin/MENU-IGATE
sudo chmod +x /bin/menu-igate
sudo chmod +x /bin/MENU-IGATE
sudo chmod 755 /lib/systemd/system/daprs-board.service
sudo chmod 755 /lib/systemd/system/daprs.service
sudo systemctl daemon-reload
/usr/bin/python3 -m pip install --upgrade -r requirements.txt

# Registra el final en /opt/curl.txt
echo "Finalizado: $SCRIPT_NAME" >> /opt/curl.txt
