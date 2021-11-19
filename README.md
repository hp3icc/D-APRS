# D-APRS
Simple igate dmr , original code kf7eel

#

Este es un igate dmr de aprs , preconfigurado para utilizar en servidores freedmr , pero puede ser utilizado en otro tipo de servidores segun su requerimiento .

#

una vez instalado abrira el menu-aprs para que configure y ponga en marcha su igate 

# Nota Importante

* para que funcione correctamente , primero asegurese de tener la ultima version de freedmr en su server

* Preferiblemente instale sobre el mismo servidor freedmr 

* en la configuracion de freedmr cambiar el valor de ( GENERATOR: 100 ) por ( GENERATOR: 101 ) esto permitira una coneccion estable local del igate a su server

* Muy importante configurar con sus datos antes de poner en marcha

* se recomienda configure su  id de igate con su numero de MCC o servidor mas 999 , aunque puede colocar el de su preferencia

#

# Pre-Requisitos

* sudo

* wget

#

# Instalacion

desde su terminal o consola ssh ejecute el bash de auto instalacion 

sudo bash -c "$(wget -O - https://github.com/hp3icc/d-aprs/raw/main/install.sh)"
