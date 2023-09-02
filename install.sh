#!/bin/bash

# Verificar si el usuario tiene permisos de root
if [[ $EUID -ne 0 ]]; then
    echo "Este script debe ejecutarse como usuario ROOT"
    exit 1
fi
# Actualizar la lista de paquetes una vez al principio
 apt-get update && apt-get upgrade -y

# Lista de aplicaciones para verificar e instalar
apps=("sudo" "curl" "git" "make" "build-essential" "libusb-1.0-0-dev" "python" "python3" "python3-pip" "chkconfig" "git-core" "libi2c-dev" "i2c-tools" "lm-sensors" "python3-websockets" "python3-gpiozero" "python3-psutil" "python3-serial" "wget" "sudo" "python3-dev" "python3-venv" "libffi-dev" "libssl-dev" "cargo" "pkg-config" "sed" "default-libmysqlclient-dev" "libmysqlclient-dev" "build-essential" "zip" "unzip" "python3-distutils" "python3-twisted" "python3-bitarray" "rrdtool" "openssl" "wavemon" "gcc" "g++" "cmake" "libasound2-dev" "libudev-dev" "gpsd" "libgps-dev" "gpsd-clients" "gpsd-tools" "chrony")

# Función para verificar e instalar una aplicación
check_and_install() {
    app=$1
    if ! dpkg -s $app 2>/dev/null | grep -q "Status: install ok installed"; then
        echo "$app no está instalado. Instalando..."
        sudo apt-get install -y $app
        echo "$app instalado correctamente."
    else
        echo "$app ya está instalado."
    fi
}

# Verificar e instalar cada aplicación
for app in "${apps[@]}"; do
    check_and_install $app
done

# Verificar y actualizar python3-venv si no está instalado
if ! dpkg -s python3-venv >/dev/null 2>&1; then
    echo "python3-venv no está instalado. Instalando..."
    apt-get install python3-venv -y
    echo "python3-venv instalado correctamente."
fi

# Crear y activar un entorno virtual
cd /opt/
python3 -m venv myenv
source myenv/bin/activate

# Instalar pip en el entorno virtual
if [ -f "/opt/get-pip.py" ]
then
   rm /opt/get-pip.*
fi
wget https://bootstrap.pypa.io/pip/get-pip.py
python3 get-pip.py
rm get-pip.*

# Instalar paquetes en el entorno virtual
apt-get install -y libssl-dev

python3 -m pip install pip setuptools
python3 -m pip install cryptography Twisted bitstring MarkupSafe bitarray configparser aprslib attrs wheel service_identity pyOpenSSL mysqlclient tinydb ansi2html mysql-connector-python pandas xlsxwriter cursor pynmea2 maidenhead flask folium mysql-connector resettabletimer setproctitle requests libscrc Pyro5

# Desactivar el entorno virtual
deactivate

#pip install cryptography pyopenssl autobahn Twisted bitstring MarkupSafe bitarray configparser aprslib attrs wheel service_identity pyOpenSSL mysqlclient tinydb ansi2html mysql-connector-python pandas xlsxwriter cursor pynmea2 maidenhead flask folium mysql-connector resettabletimer setproctitle requests libscrc Pyro5

cd /opt
# Instalar Rust y configurar versión
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

rustup install 1.72.0
rustup default 1.72.0

/usr/bin/python3 -m pip install --upgrade pyOpenSSL
/usr/bin/python3 -m pip install --upgrade autobahn
/usr/bin/python3 -m pip install --upgrade jinja2
/usr/bin/python3 -m pip install --upgrade dmr-utils3
/usr/bin/python3 -m pip install --upgrade ansi2html
/usr/bin/python3 -m pip install --upgrade aprslib
/usr/bin/python3 -m pip install --upgrade tinydb
/usr/bin/python3 -m pip install --upgrade mysqlclient
/usr/bin/python3 -m pip install --upgrade setproctitle
/usr/bin/python3 -m pip install --upgrade pynmea2
echo "Instalación completa."

####################
cd /opt/
git clone https://github.com/hp3icc/D-APRS.git

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
nano /opt/D-APRS/gps_data.cfg ;;
2)
sudo systemctl stop daprs.service && sudo systemctl start daprs.service && sudo systemctl enable daprs.service ;;
3)
sudo systemctl stop daprs.service && sudo systemctl disable daprs.service ;;
4)
sudo systemctl stop daprs-board.service && systemctl start daprs-board.service && sudo systemctl enable daprs-board.service ;;
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
sudo chmod +777 /opt/D-APRS/user_settings.txt
sudo chmod +x /opt/D-APRS/dashboard/*.py
sudo chmod +x /opt/D-APRS/*.py
sudo chmod +x /bin/menu-igate
sudo chmod 755 /lib/systemd/system/daprs-board.service
sudo chmod 755 /lib/systemd/system/daprs.service
sudo systemctl daemon-reload

menu-igate
