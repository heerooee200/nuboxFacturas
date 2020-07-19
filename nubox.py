import requests 
import queue
import threading
import ast
import pyodbc
import time

from lxml import etree as ET
from lxml_to_dict import lxml_to_dict
from datetime import timedelta, date

class NuboxApi:
    'Clase para conexion a la API'

    dsUsuarioApi    = "*******"
    dsPasswordApi   = "*******"
    dsB64AuthHeader = "*******"
    dsUrlApi        = "https://api.nubox.com/Nubox.API.Cert/"
    dsToken         = ""

    def __init__(self):
        self.refreshToken()

    def refreshToken(self):
        requestURL   = self.dsUrlApi+"autenticar"

        r = requests.post( requestURL , headers = {'content-type': 'application/json', 'authorization':'Basic '+self.dsB64AuthHeader} )
        self.dsToken = r.headers['Token']

    def getFacturaDia(self,fecha,tipo):
        requestURL   = self.dsUrlApi+"factura/documento/77078349-6/venta/"+fecha+"/"+tipo+"/0/1"
        headers      = {'content-type': 'application/json', 'token' :self.dsToken }

        r = requests.get( requestURL , headers = headers )
        return r.content


# Recorre un rango de fechas
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

# Retorna un valor si existe la clave en el diccionario
def getValueDict(key,dict):
    val = None
    if key in dict.keys(): 
            val = dict[key]
    return val

#Crea hilos para cada tipo de factura y retorna un diccionario
def ObtenerFacturas(nubox,fechaInicio,fechaFin):
    FACTURAS  = {}
    total    = 6
    hilos    = []
    tiposfacturas = ["FAC-EE","FAC-EL","N%252FD-EL","N%252FC-EL","BOL-EL","BOL-EE"]
    out_que = queue.Queue()
    parser = ET.XMLParser(recover=True)

    for cont in range(0,total):         
        hilos.append(threading.Thread(name = "Thread "+str(cont) ,target = factura_hilo, args = (out_que,parser,tiposfacturas[cont],fechaInicio,fechaFin,nubox)))
    for cont in range(0,total):
        hilos[cont].start()
    for cont in range(0,total):
        hilos[cont].join()

    cont = 0 
    while not out_que.empty():
        result = out_que.get()
        FACTURAS[cont] = result
        cont = cont + 1

    return FACTURAS

#Obtiene las facturas de una fecha y las inserta en una Queue que comparte con los otros hilos
def factura_hilo(out_que, parser, tipo , fechaInicio, fechaFin, nubox):  
    
    for single_date in daterange(fechaInicio, fechaFin):
        facturasDia = nubox.getFacturaDia(single_date.strftime("%Y-%m-%d"),tipo)
        if facturasDia != b'"No se encontraron registros."' :
            facturasDia  = ast.literal_eval(facturasDia.decode('UTF-8'))
            for factura in facturasDia: 
                facturaDict  = lxml_to_dict(ET.fromstring(factura , parser = parser ))
                out_que.put(facturaDict)

#Inserta las facturas en la BD sql Server
def insertFacturas(cursor,facturasDict):
    for nFact, factura in facturasDict.items():    
        try:
            RUTEmisor = factura['Documento']['Encabezado']['Emisor']['RUTEmisor']

            cursor.execute('SELECT COUNT(*) FROM dbo.EMPRESA WHERE dcRut = (?)', RUTEmisor)
            number_of_rows = cursor.fetchone()[0]

            if(number_of_rows == 0):
                cursor.execute("INSERT INTO dbo.EMPRESA VALUES(?,?,?,?,?)",RUTEmisor,factura['Documento']['Encabezado']['Emisor']['RznSoc'],factura['Documento']['Encabezado']['Emisor']['GiroEmis'],factura['Documento']['Encabezado']['Emisor']['DirOrigen'],factura['Documento']['Encabezado']['Emisor']['CmnaOrigen'])
                conn.commit()

            cursor.execute('SELECT pnCodEmp FROM dbo.EMPRESA WHERE dcRut = (?)', RUTEmisor)
            CodEmisor = cursor.fetchone()[0]

            RUTRecep = factura['Documento']['Encabezado']['Receptor']['RUTRecep']

            cursor.execute('SELECT COUNT(*) FROM dbo.EMPRESA WHERE dcRut = (?)', RUTRecep)
            number_of_rows = cursor.fetchone()[0]

            if(number_of_rows == 0):
                cursor.execute("INSERT INTO dbo.EMPRESA VALUES(?,?,?,?,?)",RUTRecep,factura['Documento']['Encabezado']['Receptor']['RznSocRecep'],factura['Documento']['Encabezado']['Receptor']['GiroRecep'],factura['Documento']['Encabezado']['Receptor']['DirRecep'],factura['Documento']['Encabezado']['Receptor']['CmnaRecep'])
                conn.commit()

            cursor.execute('SELECT pnCodEmp FROM dbo.EMPRESA WHERE dcRut = (?)', RUTRecep)
            CodRecep = cursor.fetchone()[0]

            TipoDTE         = getValueDict('TipoDTE',factura['Documento']['Encabezado']['IdDoc'])
            Folio           = getValueDict('Folio',factura['Documento']['Encabezado']['IdDoc'])
            FchEmis         = getValueDict('FchEmis',factura['Documento']['Encabezado']['IdDoc'])
            TpoTranCompra   = getValueDict('TpoTranCompra',factura['Documento']['Encabezado']['IdDoc'])
            TpoTranVenta    = getValueDict('TpoTranVenta',factura['Documento']['Encabezado']['IdDoc'])
            IndServicio     = getValueDict('IndServicio',factura['Documento']['Encabezado']['IdDoc'])
            FmaPago         = getValueDict('FmaPago',factura['Documento']['Encabezado']['IdDoc'])
            PeriodoDesde    = getValueDict('PeriodoDesde',factura['Documento']['Encabezado']['IdDoc'])
            PeriodoHasta    = getValueDict('PeriodoHasta',factura['Documento']['Encabezado']['IdDoc'])
            MedioPago       = getValueDict('MedioPago',factura['Documento']['Encabezado']['IdDoc'])
            TermPagoDias    = getValueDict('TermPagoDias',factura['Documento']['Encabezado']['IdDoc'])
            FchVenc         = getValueDict('FchVenc',factura['Documento']['Encabezado']['IdDoc'])

            MntExe           = getValueDict('MntExe',factura['Documento']['Encabezado']['Totales'])
            MntTotal         = getValueDict('MntTotal',factura['Documento']['Encabezado']['Totales'])
            
            
        
            cursor.execute('SELECT COUNT(*) FROM dbo.ENCABEZADO_FACTURA WHERE dnFolio = (?) AND dnTipDoc = (?) AND ddFecEmision = (?)', Folio, TipoDTE,FchEmis)
            number_of_rows = cursor.fetchone()[0]

            if(number_of_rows == 0):
                cursor.execute("INSERT INTO dbo.ENCABEZADO_FACTURA VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",TipoDTE,Folio,FchEmis,TpoTranCompra,TpoTranVenta,CodEmisor,CodRecep,MntExe,MntTotal,FchVenc,IndServicio,FmaPago,PeriodoDesde,PeriodoHasta,MedioPago,TermPagoDias)
                conn.commit()

            cursor.execute('SELECT pnCodFact FROM dbo.ENCABEZADO_FACTURA WHERE dnFolio = (?) AND dnTipDoc = (?) AND ddFecEmision = (?)', Folio, TipoDTE, FchEmis)
            codFactura = cursor.fetchone()[0]

            listDetalles = [i for i in factura['Documento'].keys() if 'Detalle' in i] 

            for lDet in listDetalles:
                Detalle        = factura['Documento'][lDet]
                NroLinDet      = getValueDict('NroLinDet',Detalle) 
                IndExe         = getValueDict('IndExe'   ,Detalle) 
                NmbItem        = getValueDict('NmbItem'  ,Detalle) 
                QtyItem        = getValueDict('QtyItem'  ,Detalle) 
                PrcItem        = getValueDict('PrcItem'  ,Detalle) 
                MontoItem      = getValueDict('MontoItem',Detalle) 

                TpoCodigo      = None
                VlrCodigo      = None
                if getValueDict('CdgItem', Detalle) is not None :
                    TpoCodigo      = getValueDict('TpoCodigo',Detalle['CdgItem']) 
                    VlrCodigo      = getValueDict('VlrCodigo',Detalle['CdgItem']) 

                cursor.execute("INSERT INTO dbo.DETALLE_FACTURA VALUES (?,?,?,?,?,?,?,?,?)",codFactura,NroLinDet,TpoCodigo,VlrCodigo,IndExe,NmbItem,QtyItem,PrcItem,MontoItem)
                conn.commit()
        except:
            print("ERROR AL INSERTAR UNA FACTURA")

        


#Ejecucion
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=*******;DATABASE=*******;UID=*******;PWD=*******')
cursor = conn.cursor()

nubox = NuboxApi()

print("Token")
print(nubox.dsToken)

fechaInicio = date(2020, 1, 1)  
fechaFin    = date(2020, 7, 1)

facturasDict = ObtenerFacturas(nubox,fechaInicio,fechaFin)
insertFacturas(cursor,facturasDict)







