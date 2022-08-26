#!/usr/bin/python -u
# -*- coding: utf-8 -*-

from bottle import abort, request, response, get, post, route, template, redirect, run, HTTPError, error
from functools import wraps
import psycopg2 as pg
from email.message import Message
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import mimetypes
import email.utils
from email import encoders
import smtplib
import json
import re
import logging
from logging.handlers import TimedRotatingFileHandler
from logging import StreamHandler
import sys
import os
import bottle

# logging.basicConfig(filename='/var/log/spedizioniere/spedizioniere.log',level=logging.DEBUG,format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')

logger = logging.getLogger("spedizioniere")
fmt=logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
_TEST_ = False
if _TEST_:
    hnd = StreamHandler()
else:
    hnd = TimedRotatingFileHandler('/var/log/spedizioniere/spedizioniere.log', when='midnight', interval=1,
                                   backupCount=31)
hnd.setFormatter(fmt)
if not logger.handlers:
    logger.addHandler(hnd)
logger.setLevel(logging.DEBUG)

sys.path.append(os.path.dirname(__file__))
import db_config


class SpedizioniereException(Exception):
    pass


class ShpeckCommunicatorException(SpedizioniereException):
    pass


dbInt = db_config.dbInt
PERMISSION_WRITE = 2
PERMISSION_READ = 4
STATUS_MAP = {
    '0': "RECEIVED",
    '1': "SENT",
    '2': "TO_SEND",
    '3': "WAIT_FOR_RECEPIT",
    '4': "ERROR",
    '5': "CONFIRMED",
    '6': "ACCEPTED",
}

SHPECK_PREFIX = "shpk_"

def checkmail_syntax(mail):
    logger.debug("dentro checkmail_syntax...")
    # logger.debug("checking :%s"%str(mail))
    for m in mail:
        logger.debug("checking :%s" % m[1])
        # if not re.match(r'[^@ ]+@[^@ ]+\.[^@ ]+',m[1]):
        if not re.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', m[1]):
            raise ShpeckCommunicatorException("Email non valida %s" % m[1])


# fa una select sul db internauta e torna true/false se la pec è attiva e non chiusa
def is_pec_attiva_internauta(indirizzo):
    logger.debug("is_pec_attiva_internauta %s ..." % indirizzo)
    # query pec internauta
    qsi = "select id, attiva, chiusa from baborg.pec where indirizzo = %s "
    c = get_internauta_cursor()
    c.execute(qsi, (indirizzo,))
    row = c.fetchone()
    if row == None:
        logger.debug("is_pec_attiva_internauta Row is None")
        c.close()
        return False
    logger.debug("Valori trovati (id, attiva, chiusa): " + str(row))
    id, attiva, chiusa = row
    intern_active = False
    if attiva:
        logger.debug("indirizzo %s attivo in internauta" % str(indirizzo))
        intern_active = True
    else:
        logger.debug("indirizzo %s non attivo in internauta: bisogna usare il pecgw locale" % str(indirizzo))
    c.close()
    return intern_active


# fa una select sul db locale (dopo aver stabilito una connessione) e restituisce true/false se la mail_config risulta 'enabled'
def is_pec_attiva_locale(indirizzo):
    logger.debug("is_pec_attiva_locale %s ..." % indirizzo)
    # query pec locale
    qsl = "select id, enabled from pecgw.mail_config where description = %s "
    # devo creare una connessione locale
    dbloc = db_config.db
    connection = pg.connect(host=dbloc['host'], user=dbloc['username'], password=dbloc['password'],
                            database=dbloc['db'], port=dbloc['port'])
    c = connection.cursor()
    c.execute(qsl, (indirizzo,))
    row = c.fetchone()
    logger.debug("Valori trovati (id, enabled): " + str(row))
    if row == None:        
        logger.debug("is_pec_attiva_locale Row is None")
        c.close()
        return False

    id, enabled = row
    local_active = False
    if enabled:
        logger.debug("indirizzo %s attivo in locale" % str(indirizzo))
        local_active = True
    else:
        logger.debug("indirizzo %s non attivo in locale: bisogna usare il lo shpeck" % str(indirizzo))

    connection.close()
    c.close()
    return local_active


# verifica che la pec sia attiva in internauta
def is_pec_switched(indirizzo):
    logger.debug("is_pec_switched %s ..." % indirizzo)
    logger.debug("Verifico se l'indirizzo è attivo in internauta...")
    interactive = is_pec_attiva_internauta(indirizzo)
    logger.debug("Verifico se l'indirizzo è attivo in locale...")
    locactive = is_pec_attiva_locale(indirizzo)

    risposta = False
    if (interactive and not (locactive)):
        risposta = True
    elif (not (interactive and locactive)):
        risposta = False
    else:
        logger.error("Errore la pec usata non switchata bene")
        logger.error("Valore attivo in internauta %s" %str(interactive))
        logger.error("Valore attivo in locale %s" %str(locactive))
        #raise ShpeckCommunicatorException("Pec attiva o disattiva su entrambi gli ambienti: verificare!")
        return False
    return risposta


# restituisce una connessione con il database di internauta (in base alle impostazioni db_config)
def get_internauta_connection(host=dbInt['host'], username=dbInt['username'], password=dbInt['password'],
                              db=dbInt['db'], port=dbInt['port']):

    try:
        intConnection = pg.connect(host=host, user=username, password=password, database=db, port=port)
        return intConnection
    except Exception as e:
        logger.error("Errore nel creare la connessione internauta ", str(e))
        raise e


# cerca di stabilire una connessione col database internauta e ne torna un cursore
def get_internauta_cursor():
    try:
        connection = get_internauta_connection()
        return connection.cursor()
    except Exception as e:
        logger.error("Errore nel get_internauta_cursor(): ", str(e))


# ritorna la chiave relativa allo status passato come parametro dalla mappa degli stati (STATUS_MAP)
def get_status_id(status):
    id = None
    for status_id, status_name in STATUS_MAP.items():
        if status_name == status:
            id = status_id
    return id


# verifica lo status del message in internauta passando l'id dell'outbox
def get_status_from_shpeck_by_outbox(outbox):
    try:
        logger.debug("get_status_from_shpeck_by_outbox(outbox): outbox = %s", outbox)
        qs = """
            -- da outbox
            select o.id as outbox, m.id as id_message, m.message_status, p.indirizzo, o.ignore
            from shpeck.outbox o
            left join shpeck.messages m on m.id_outbox = o.id
            left join baborg.pec p on  p.id = o.id_pec
            where o.id = %s
                UNION
            -- da messages
            select o.id as outbox, m.id as id_message, m.message_status, p.indirizzo, o.ignore
            from shpeck.messages m
            left join shpeck.outbox o on o.id = m.id_outbox
            left join baborg.pec p on p.id = m.id_pec
            where m.id_outbox = %s
        """

        c = get_internauta_cursor()
        c.execute(qs, (outbox, outbox,))
        field_names = [column[0] for column in c.description]
        logger.debug(field_names)

        res = c.fetchall()

        if len(res) > 1:
            raise ValueError("ci sono troppi riferimenti allo stesso outbox")
        elif len(res) == 0:
            raise ValueError("outbox non trovato")

        id_message = None
        status = None

        for r in res:
            logger.debug(r)
            id_message = r[1]
            status = get_status_id(r[2])
        c.close()

        # Se ho trovato la riga ma non ho questi valori, allora vuol dire che l'outbox è ancora da spedire
        if id_message is None and status is None:
            status = get_status_id("TO_SEND")

        return id_message, status
        # finito

    except ValueError as ve:
        logger.error(ve)
        c.close()

# parsa l'id_message rimuovendo il segnaposto internauta (lo SHPECK_PREFIX) e richiede a internauta lo status del messaggio
def get_message_status(messageId):
    logger.debug("shpeck-communicator -> get_message_status, message: %s" % messageId)
    outbox = messageId[len(SHPECK_PREFIX):]  # quello ottenuto è l'id della tabella shpeck.outbox
    logger.debug("substringed outbox da cercare: %s" % outbox)
    id_message, status = get_status_from_shpeck_by_outbox(outbox)
    return id_message, status

def translate_shpeck_recepit_to_spedizioniere_recepit(recepit_type):
    if(recepit_type == "ACCETTAZIONE"):
        return "accettazione"
    elif(recepit_type == "PREAVVISO_ERRORE_CONSEGNA"):
        return "preavviso-errore-consegna"
    elif (recepit_type == "NON_ACCETTAZIONE"):
        return "non_accettazione"
    elif (recepit_type == "RILEVAZIONE_VIRUS"):
        return "rilevazione"
    elif (recepit_type == "ERRORE_CONSEGNA"):
        return "errore-consegna"
    elif (recepit_type == "CONSEGNA"):
        return "avvenuta-consegna"
    else:
        return "unknown"


# chiede al db internauta le ricevute del messaggio a cui corrisponde l'oubox
def get_recepit_from_shpeck(outbox):
    logger.debug("shpeck-communicator -> get_recepit_from_shpeck message: %s" % outbox)

    q = """ select mr.uuid_repository,r.recepit_type,p.indirizzo 
            from shpeck.messages mr 
            join shpeck.recepits r on (mr.id=r.id)
            join shpeck.messages m on m.id = mr.id_related 
            join baborg.pec p on p.id = m.id_pec  
            where m.id_outbox=%s and mr.message_type='RECEPIT'  
            order by mr.id
            """

    outboxed = outbox
    outboxed = outboxed.replace(SHPECK_PREFIX,"")
    logger.debug("Outbox da cercare %s" %str(outboxed))
    c = get_internauta_cursor()
    c.execute(q, (str(outboxed),))
    logger.debug("ok ora fetcho")
    rows = c.fetchall()
    logger.debug("fetchall-> %s" %str(rows))
    if rows is None:
        c.close()
        return None, None
    recepits = []
    mail = None
    for r in rows:
        recepits.append({'uuid': r[0], 'tipo': translate_shpeck_recepit_to_spedizioniere_recepit(r[1])})
        mail = r[2]
        c.close()
    logger.debug("ritorno mail %s : " %str(mail) )
        
    logger.debug("ritorno recepits %s : " %str(recepits))    
    
    return mail, recepits
    #return mail, recepits


# parsa l'id_message rimuovendo il segnaposto internauta (SHPECK_PREFIX) e chiede a internauta lo status del messaggio
def get_recepit(messageId):
    logger.debug("shpeck-communicator -> get_recepit message: %s" % messageId)
    outbox = messageId[len(SHPECK_PREFIX):]  # quello ottenuto è l'id della tabella shpeck.outbox
    logger.debug("substringed outbox da cercare: %s" % outbox)
    mail, recepits = get_recepit_from_shpeck(outbox)
    return mail, recepits


# chiede al db internauta le ricevute di errore del messaggio a cui corrisponde l'oubox
def get_error_recepits_from_shpeck(outbox):
    logger.debug("shpeck-communicator -> get_error_recepits_from_shpeck message: %s" % outbox)
    q = """ select rm.uuid_repository, r.recepit_type 
            from shpeck.messages rm
            join shpeck.messages m on m.id = rm.id_related
            join shpeck.recepits r on r.id = rm.id
            where m.id_outbox = %s
            and r.recepit_type in ('PREAVVISO_ERRORE_CONSEGNA','NON_ACCETTAZIONE','RILEVAZIONE_VIRUS','ERRORE_CONSEGNA')
            order by rm.id
        """

    c = get_internauta_cursor()
    c.execute(q, (outbox,))
    rows = c.fetchall()
    if not rows:
        c.close()
        return None
    recepits = []
    for r in rows:
        recepits.append({'uuid': r[0], 'tipo': translate_shpeck_recepit_to_spedizioniere_recepit(r[1])})
    c.close()
    logger.debug("recepits %s", str(recepits))
    return recepits


# parsa l'id_message rimuovendo il segnaposto internauta (SHPECK_PREFIX) e chiede le sue ricevute di errore a internauta
def get_error_recepits(messageId):
    logger.debug("shpeck-communicator -> get_error_recepits message: %s" % messageId)
    outbox = messageId[len(SHPECK_PREFIX):]  # quello ottenuto è l'id della tabella shpeck.outbox
    logger.debug("substringed outbox da cercare: %s" % outbox)
    recepits = get_error_recepits_from_shpeck(outbox)
    return recepits


def store_message_in_db_internauta(raw_message, indirizzo, external_id):
    logger.debug("shpeck-communicator -> store_message_in_db_internauta...")
    qp = "select id from baborg.pec where indirizzo = %s"
    qo = "insert into shpeck.outbox (id_pec,raw_data,ignore,id_applicazione,external_id ) values (%s,%s,false,'procton',%s) returning id"

    to = [email.utils.parseaddr(x) for x in raw_message.get('To', '').split(',') if x != '']
    cc = [email.utils.parseaddr(x) for x in raw_message.get('Cc', '').split(',') if x != '']
    if not to:
        raise ShpeckCommunicatorException("Nessun indirizzo di destinazione valido")
    checkmail_syntax(to)
    checkmail_syntax(cc)
    logger.debug("Mi creo una connessione internauta")
    connection = get_internauta_connection()
    logger.debug("Prendo il cursore")
    c = connection.cursor()
    try:
        logger.debug("recupero l'id dell'indirizzo da baborg")
        c.execute(qp, (str(indirizzo),))
        logger.debug("Eseguira ora fetcho")
        row = c.fetchone()
        
        logger.debug("recuperato %s " %str(row))
        id_pec = row[0]
        # converto per sicurezza il messaggio
        try:
            message_string = raw_message.as_string().decode('UTF-8')
        except:
            message_string = raw_message.as_string().decode('latin1')

        logger.debug("ho questo external_id: " + str(external_id))
        logger.debug("ho questo external_id: " + str(external_id))
        c.execute(qo, (id_pec, message_string,external_id))
        result = c.fetchone()
        logger.debug("result %s" %str(result))
        message_id = result[0]

    except Exception as e:
        logger.error("Errore: %s" %str(e))
        connection.rollback()
        c.close()
        raise SpedizioniereException("Errore nell'inserimento del messaggio su internauta: " + str(e))

    connection.commit()
    c.close()
    connection.close()
    logger.debug("ho questo id: " + str(message_id) + " ma lo devo 'shpeckare'")
    message_id = SHPECK_PREFIX +  str(message_id)
    logger.debug("ritorno " + str(message_id))
    return message_id
