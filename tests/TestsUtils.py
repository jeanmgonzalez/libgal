from getpass import getpass


def ask_user_pwd():
    host = input(f'Ingrese host a conectarse: ')
    logmech = 'LDAP' if input(f'Debería usar LDAP para autenticar (s/n)?: ').strip().lower() == 's' else 'TD2'
    usr = input(f'Ingrese usuario de conexión: ')
    passw = getpass(f'Ingrese la contraseña para el usuario {usr}: ')
    return host, usr, passw, logmech

