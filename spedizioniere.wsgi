#coding: utf-8

# -*- coding: utf-8 -*-


from bottle import abort,request,response,get,post,route,template,redirect,run,HTTPError,error
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
import logging
from logging.handlers import TimedRotatingFileHandler
from logging import StreamHandler
import sys
import os
import bottle

#proc = os.path.basename(__file__)
proc = os.getcwd()

logger=logging.getLogger("spedizioniere")
fmt=logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
_TEST_=False
if _TEST_ :
    hnd=StreamHandler()
else:
    hnd=TimedRotatingFileHandler('/var/log/spedizioniere/spedizioniere.log',when='midnight',interval=1,backupCount=31)
hnd.setFormatter(fmt)
logger.addHandler(hnd)
logger.setLevel(logging.DEBUG)
sys.path.append(os.path.dirname(__file__))
import db_config
__db=db_config.db
import re

import shpeck_communicator as sc

PERMISSION_WRITE=2
PERMISSION_READ=4
STATUS_MAP={
            '0':"RECEIVED",
            '1':"SENT",
            '2':"TO_SEND",
            '3':"WAIT_FOR_RECEPIT",
            '4':"ERROR",
            '5':"CONFIRMED",
            '6':"ACCEPTED",
            }

SHPECK_PREFIX = "shpk_"
def is_ascii(s):
    return all(ord(c) < 128 for c in s)

class SpedizioniereException(Exception):
    pass


def db_connect(host=__db['host'],username=__db['username'],password=__db['password'],db=__db['db'],port=__db['port']):
    conn=pg.connect(host=host,user=username,password=password,database=db,port=port)
    return conn

def has_permission(db,app_id,mail,permission):
    logger.debug("entrato in has_permission(db,app_id,mail,permission):")
    q="select permission from permission join mail_config  on mail_config.id= permission.mail_config where external_app = %s and description=%s"
    c=db.cursor()
    c.execute(q,(app_id,mail))
    rows=c.fetchall()
    if len(rows) != 1:
        c.close()
        return False
    perm=rows[0][0]
    c.close()
    return perm&permission

def check_mail_id(db,mail):
    logger.debug("entrato in check_mail_id(db,mail): "+str(mail))
    q="select id from mail_config where description=%s"
    c=db.cursor()
    c.execute(q,(mail,))
    rows=c.fetchall()
    if len(rows) != 1:
        c.close()
        return False
    mail_id=rows[0][0]
    c.close()
    return mail_id

def check_external_id(db,external_id,app_id):
    logger.debug("entrato in check_external_id(db,external_id,app_id):")
    q="select id from messages where external_id=%s and sender_app=%s"
    c=db.cursor()
    c.execute(q,(external_id,app_id))
    row=c.fetchone()
    if row:
        return row[0]
    return None

def store_addresses(db,message_id,f,to,cc):
    logger.debug("entrato in store_addresses(db,message_id,f,to,cc):")
    c=db.cursor()
    qa="insert into addresses (address,original_address) values (%s,%s) returning id"
    qma="insert into messages_addresses (message,address,\"role\") values(%s,%s,%s)"
    qida="select id  from addresses where address=%s"
    for a in f:
        try:
            c.execute("savepoint store_address")
            c.execute(qa,(a[1],a[0]))
            address_id=c.fetchone()[0]
            c.execute("release savepoint store_address")
        except pg.IntegrityError:
            c.execute("rollback to savepoint store_address")
        c.execute(qida,(a[1],))
        address_id=c.fetchone()[0]
        c.execute(qma,(message_id,address_id,0))

    for a in to:
        try:
            c.execute("savepoint store_address")
            c.execute(qa,(a[1],a[0]))
            address_id=c.fetchone()[0]
            c.execute("release savepoint store_address")
        except pg.IntegrityError:
            c.execute("rollback to savepoint store_address")
        c.execute(qida,(a[1],))
        address_id=c.fetchone()[0]
        c.execute(qma,(message_id,address_id,1))
    for a in cc:
        try:
            c.execute("savepoint store_address")
            c.execute(qa,(a[1],a[0]))
            address_id=c.fetchone()[0]
            c.execute("release savepoint store_address")
        except pg.IntegrityError:
            c.execute("rollback to savepoint store_address")
        c.execute(qida,(a[1],))
        address_id=c.fetchone()[0]
        c.execute(qma,(message_id,address_id,2))

    c.close()

def checkmail_syntax(mail):
    #logger.debug("checking :%s"%str(mail))
    for m in mail:
        logger.debug("checking :%s"%m[1])
        #if not re.match(r'[^@ ]+@[^@ ]+\.[^@ ]+',m[1]):
        if not re.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$',m[1]):
            raise SpedizioniereException("Email non valida %s"%m[1])

def store_message_in_db(db,m,app_id,mail_id,external_id):
    logger.debug("entrato in store_message_in_db(db,m,app_id,mail_id,external_id):")
    qm="insert into messages (message_id,origin,sender_app,subject,status,\"inout\",external_id) values (%s,%s,%s,%s,%s,%s,%s) returning id"
    qo="insert into outbox (message,raw_message,\"type\",external_app) values (%s,%s,%s,%s)"
    to=[email.utils.parseaddr(x) for x in m.get('To','').split(',') if x != '']
    cc=[email.utils.parseaddr(x) for x in m.get('Cc','').split(',') if x != '']
    if not to:
        raise SpedizioniereException("Nessun indirizzo di destinazione valido")
    checkmail_syntax(to)
    checkmail_syntax(cc)
    c=db.cursor()
    try:
        c.execute(qm,(m['Message-Id'],mail_id,app_id,m['Subject'],2,1,external_id))
        message_id = c.fetchone()[0]
        try:
            message_string=m.as_string().decode('UTF-8')
        except:
            message_string=m.as_string().decode('latin1')
        c.execute(qo,(message_id,message_string,0,app_id))
        store_addresses(db,message_id,[(m['From'],m['From'])],to,cc)
    except:
        c.close()
        raise
    c.close()
    return message_id

def db_get_status(db,message_id):
    logger.debug("entrato in  db_get_status(db,message_id):")
    q="select mc.description,m.status from mail_config mc join messages m on mc.id=m.origin where m.id=%s"
    c=db.cursor()
    c.execute(q,(message_id,))
    row=c.fetchone()
    if not row:
        return None,None
    return row

def db_get_recepit(db,message_id):
    logger.debug("entrato in db_get_recepit(db,message_id):")
    q="select uuid_doc,pec_ricevuta,description from messages m join mail_config mc on (mc.id=m.origin)  where related=%s and recepit=true"
    c=db.cursor()
    c.execute(q,(message_id,))
    rows=c.fetchall()
    if not rows:
        c.close()
        return None,None
    recepits=[]
    mail=None
    for r in rows:
        recepits.append({'uuid':r[0],'tipo':r[1]})
        mail=r[2]
    c.close()
    return mail,recepits

def db_get_error_recepit(db,message_id):
    logger.debug("entrato in db_get_error_recepit(db,message_id):")
    q="""select r.uuid_doc,r.pec_ricevuta from pecgw.messages m  join pecgw.messages r on (m.id=r.related)
where
 m.status=4 and
 m.id=%s and
 r.pec_ricevuta in ('non-accettazione','rilevazione-virus','errore-consegna')"""
    c=db.cursor()
    c.execute(q,(message_id,))
    rows=c.fetchall()
    if not rows:
        c.close()
        return None
    recepits=[]
    for r in rows:
        recepits.append({'uuid':r[0],'tipo':r[1]})
    c.close()
    return recepits

def db_unset_external_id(db,message_id):
    logger.debug("entrato in db_unset_external_id(db,message_id):")
    q="update messages set external_id=null , status=7 where id=%s"
    c=db.cursor()
    c.execute(q,(message_id,))
    c.close()

def db_get_emails(db,app_id):
    logger.debug("entrato in db_get_emails(db,app_id):")
    q="select description,permission from permission join mail_config  on mail_config.id= permission.mail_config where external_app = %s"
    c=db.cursor()
    c.execute(q,(app_id,))
    rows=c.fetchall()
    if rows:
        mails=[x[0]  for x in rows if x[1] & PERMISSION_WRITE]
        return mails
    return None

def db_get_emails_with_permission(db,app_id):
    logger.debug("entrato in db_get_emails_with_permission(db,app_id):")
    q="select description,permission,enabled,per_riservato from permission join mail_config  on mail_config.id= permission.mail_config where external_app = %s"
    c=db.cursor()
    c.execute(q,(app_id,))
    rows=c.fetchall()
    if rows:
        mails=[{'casella':x[0],'permesso':x[1],'enabled':(x[2] or sc.is_pec_switched(x[0])),'per_riservato':x[3]}  for x in rows]
        return mails
    return None


def authenticate(conn,username,password):
    logger.debug("entrato in  authenticate(conn,username,password)")
    q="select id from external_app where username=%s and password = md5(%s)"
    c=conn.cursor()
    c.execute(q,(username,password))
    rows=c.fetchall()
    c.close()
    if len(rows) != 1:
        logger.warn("auht failed username: %s"%username)
        return False
    return rows[0][0]

def db(f):
    @wraps(f)
    def wrap(*args,**kwargs):
        kwargs['db']=db_connect()
        return f(*args,**kwargs)
    return wrap

#@db
#@auth
#def test(username,password):
  #  print "test"


@post('/sendmail')
@db
def sendmail(db):
    logger.debug(proc)
    logger.debug("entrato in '/sendmail'")
    logger.debug("sendmail: "+str(request.forms))
    logger.debug("da questo from: " + str(request.forms.mail))
    try:
        app_id=authenticate(db,request.forms.username,request.forms.password)
        if not app_id :
                abort(401,"Auth failed")
        o=""
        if not has_permission(db,app_id,request.forms.mail,PERMISSION_WRITE):
                logger.warn("Sendmail non authorized")
                abort(401,"Sendmail is not authorized")
        external_id=request.forms.external_id
        if external_id=="":
            external_id = None
        else:
            res=check_external_id(db,external_id,app_id)
            if request.forms.force and res:
                logger.info("Sendmail forced")
                db_unset_external_id(db,res)
            else:
                if res:
                    response.status=200
                    return str(res)
        mail=request.forms.mail
        mail_id=check_mail_id(db,mail)
        if not mail_id:
            logger.error("mail non valida")
            abort(400,"email non valida")

        debug=request.forms.debug
        message=request.forms.message
        if not message:
            message=""
        subject=request.forms.subject
        to=request.forms.to
        if not to:
            logger.error("mancano i destinatari")
            abort(400,"mancano i destinatari")
        cc=request.forms.cc
        #TODO check to e cc
        if not request.files:
            m=MIMEText(message,'html')
           # m.set_type("text/html")
            m['From']=mail
            m['To']=to
            m['Cc']=cc
            m['Subject']=subject
            m.set_charset("utf8")
            m['Message-Id']=email.utils.make_msgid("PecGW")
            message_id=['Message-Id']
            m['Date']=email.utils.formatdate()
        else:
            m=MIMEMultipart()
            m['From']=mail
            m['To']=to
            m['Cc']=cc
            m['Subject']=subject
            mimetext=MIMEBase('text','html')
            mimetext.set_payload(message.encode('utf8'))
            mimetext.set_charset('utf8')
            m['Message-Id']=email.utils.make_msgid("PecGW")
            message_id=['Message-Id']
            m['Date']=email.utils.formatdate()

           # mimetext['Content-Type']='text/html; charset="UTF-8"'
            #encoders.encode_7or8bit(mimetext)
            m.attach(mimetext)
      #      if is_ascii(message):
       #         m.attach(MIMEText(message,'html'))
        #    else:
         #       m.attach(MIMEText(message.encode('utf8'),'html','utf8'))
            for f in request.files:
                t=mimetypes.guess_type(request.files[f].filename)
                maintype="binary"
                subtype="octet-stream"
                if t[0]:
                    maintype, subtype = t[0].split('/', 1)
                a=MIMEBase(maintype,subtype)
                a.set_payload(request.files[f].file.read())
                if maintype=='message' and subtype=='rfc822':
                    encoders.encode_7or8bit(a)
                else:
                    encoders.encode_base64(a)
                a["Content-Disposition"]='attachment;filename="%s"'%request.files[f].filename
                m.attach(a)

        #externalId,String message,String subject,String to,String cc,

       # s=smtplib.SMTP(host='smtp')
      #  s.sendmail("noreply@ausl.bologna.it","andrea.zucchelli@cup2000.it,middleware.pec@pec.ausl.bologna.it" ,m.as_string())

        if debug:
            response.add_header("Content-Type","message/rfc822")
            response.add_header("Content-Disposition","attachment;filename=test.eml")
            return m.as_string()
        response.status=203
        logger.debug("Ora verifico se l'indirizzo è stato switchato su internauta")
        mittente = request.forms.mail
        switched_pec = sc.is_pec_switched(mittente)
        if(switched_pec):
            logger.debug("Indirizzo switchato: salvo su internauta")
            message_id = sc.store_message_in_db_internauta(m, mittente, external_id)
        else:
            logger.debug("Indirizzo NON switchato: salvo in locale")
            message_id = store_message_in_db(db,m,app_id,mail_id,external_id)
            db.commit()
            logger.debug("Salvato con id %s" % str(message_id))
        return str(message_id)
    except SpedizioniereException as e:
        abort(400,str(e))
    except HTTPError:
        raise
    except:
        logger.exception("sendmail")
        abort(500,"internal server error")

@get('/get_status/<message_id>')
@db
def get_status(message_id,db):
    logger.debug(proc)
    logger.debug("sono entrato in '/get_status/<message_id>'")
    try:
        logger.debug("get_status message: %s"%message_id)
        username=request.query.get("username")
        password=request.query.get("password")
        app_id=authenticate(db,username,password)
        if not app_id :
                logger.debug("get_status auth failed")
                abort(401,"Auth failed")

        if message_id.startswith(SHPECK_PREFIX):
            mail, status_code = sc.get_message_status(message_id)
        else:
            mail,status_code=db_get_status(db,message_id);
            #print mail,status_code
            if not has_permission(db,app_id,mail,PERMISSION_READ):
                abort(401,"get status is not authorized or message not found")
        status={'status':str(status_code)}
        if status_code==4:
            if message_id.startswith(SHPECK_PREFIX):
                recepits = sc.get_error_recepits(message_id)
            else:
                recepits=db_get_error_recepit(db,message_id)
            if recepits:
                status['recepits']=recepits
        return json.dumps(status)
    except HTTPError:
        logger.debug("sticazzi")
        logger.exception("get_status http error")
        raise
    except:
        logger.exception("get_status")
        abort(500,"internal server error")

@get('/get_recepits/<message_id>')
@db
def get_recepit(message_id,db):
    logger.debug(proc)
    logger.debug("sono entrato in '/get_recepits/<message_id>")
    try:
        logger.debug("get_recepits message: %s"%message_id)
        username=request.query.get("username")
        password=request.query.get("password")
        app_id=authenticate(db,username,password)
        if not app_id :
                abort(401,"Auth failed")
        if message_id.startswith(SHPECK_PREFIX):
            mail,recepits=sc.get_recepit_from_shpeck(message_id)
        else:
            mail,recepits=db_get_recepit(db,message_id);
        if not mail:
            abort(404,"recepit not found")
        if not has_permission(db,app_id,mail,PERMISSION_READ):
                abort(401,"get status is not authorized or message not found")
        return json.dumps(recepits)
    except HTTPError:
        raise
    except:
        logger.exception("get_recepit")
        abort(500,"internal server error")

@get('/get_emails')
@db
def get_emails(db):
    logger.debug(proc)
    logger.debug("sono entrato in /get_emails")
    try:
        username=request.query.get("username")
        password=request.query.get("password")
        app_id=authenticate(db,username,password)
        if not app_id :
            abort(401,"Auth failed")
        mail_list=db_get_emails(db,app_id);
        if not mail_list:
            return json.dumps([])
        return json.dumps(mail_list)
    except HTTPError:
        raise
    except:
        logger.exception("mail_list")
        abort(500,"internal server error")

@get('/get_emails_with_permission')
@db
def get_emails_with_permission(db):
    logger.debug('proc',str(proc))
    logger.debug("sono entrato in /get_emails_with_permission....")
    try:
        username=request.query.get("username")
        password=request.query.get("password")
        app_id=authenticate(db,username,password)
        if not app_id :
            abort(401,"Auth failed")
        mail_list=db_get_emails_with_permission(db,app_id);
        if not mail_list:
            return json.dumps([])
        return json.dumps(mail_list)
    except HTTPError:
        raise
    except:
        logger.exception("mail_list")
        abort(500,"internal server error")


@error(400)
def error400(e):
    e.content_type="text/plain"
    #response.content_type="text/plain"
    return e.body
@error(404)
def error404(e):
    #e.content_type="text/plain"
    #response.content_type="text/plain"
    return e.body
@error(401)
def error401(e):
    #e.content_type="text/plain"
    #response.content_type="text/plain"
    return e.body
@error(500)
def error500(e):
    e.content_type="text/plain"
    #response.content_type="text/plain"
    return e.body



if __name__=='__main__':
    run(host='0.0.0.0', port=8000, debug=True )#,reloader=True)
    #c=db_connect()
    #print authenticate(c,'testapp','la password')
    #test(username='testapp',password='la password')
else:
    app=bottle.default_app()
    app.cathcall=False
    application=app
