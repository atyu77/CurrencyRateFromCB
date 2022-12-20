import logging
import pyodbc
import os
import sys
import time
import zeep
import exceptions

from datetime import datetime
from datetime import timedelta
from sys import stdout


logger = logging.getLogger(__name__)

BASE_DIR = os.path.abspath(os.curdir)
RETRY_PERIOD = 1200

url = "http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL"


def get_api_answer(on_date):
    try:
        logger.info('Проверяем доступность сервиса')
        Client = zeep.Client(url)
    except Exception as error:
        message = 'Ошибка при попытке вызвать сервис: ' + str(error)
        logger.error(message)
        raise WrongAPIAnswer('Ответ сервера не является успешным')
    return (Client.service.GetCursOnDate(on_date))


def load_to_database(result, on_date):
    conn = pyodbc.connect("Driver={SQL Server};"
                          "Server=ABS13;"
                          "Database=Alorbanktest;"
                          "UID=dca;"
                          "PWD=123456"
                          )
    try:
        logger.info('Подключаемся к БД')
        cursor = conn.cursor()
    except Exception as error:
        logger.error(str(error))
    cursor.execute("delete course")

    for res in result:
        try:
            SQL = """
                  insert course(_ISO_LAT3, _SCALE, _CURSE, _DAT)
                  select '""" + str(res["ValuteCursOnDate"]["VchCode"]) + """',
                  '""" + str(res["ValuteCursOnDate"]["Vnom"]) + """',
                  '""" + str(res["ValuteCursOnDate"]["Vcurs"]) + """',
                  '"""+str(on_date)+"""'
                  """
            logger.info('Вставляем данные во временную таблицу по валюте ' +
                        str(res["ValuteCursOnDate"]["VchCode"]))
            cursor.execute(SQL)
            cursor.connection.commit()
        except Exception as error:
            logger.error(str(error))
    SQL = """
          SET NOCOUNT ON;
          SET ANSI_WARNINGS OFF;
          declare @RetVal int, @CourseUSD float, @TypeID numeric(15,0);
          select @TypeID = ID
           from tGL_ConfSet_Sync WITH (NOLOCK)
           where SysName = 'COURSETYPE'
          select @CourseUSD = convert(float,_CURSE)
            from Course
           where _ISO_LAT3 = 'USD';
          select @RetVal = 0
          if @CourseUSD = (select Course
                             from tCurrencyRate WITH(NOLOCK)
                            where ObjectID = 1
                              and TradingSysID = @TypeID
                              and Date_Time = (select max(Date_Time)
                                                 from tCurrencyRate WITH(NOLOCK)
                                                where ObjectID = 1
                                                  and TradingSysID = @TypeID
                                                  and Date_Time<='""" + str(on_date) + """'))
             select @RetVal = 1
          select @RetVal
          """
    try:
        logger.info('Запускаем проверку существования ' +
                    'курса за предыдущую дату')
        rows1 = cursor.execute(SQL).fetchall()
    except Exception as error:
        logger.error(str(error))
    if rows1[0][0] == 0:
        logger.info('Такого курса за предыдущую дату ' +
                    'нет, записываем курс в текущую дату')
        SQL = """
             SET NOCOUNT ON;
             SET ANSI_WARNINGS OFF;
             DECLARE @RetVal int;
             EXEC    @RetVal = Course_Insert @DealProtocolID= 0;
             SELECT  @RetVal;
             """

        try:
            logger.info('Запускаем процедуру по записи курсов в tCurrencyRate')
            rows = cursor.execute(SQL).fetchall()
            cursor.connection.commit()
        except Exception as error:
            logger.error(str(error))

        if rows[0][0] == 0:
            logger.info('Запись успешно произведена')
        if rows[0][0] < 0:
            logger.error('Ошибка записи курсов. ' +
                         'Курс уже существует или день закрыт')
    else:
        logger.info('Такой курс уже есть за текущую(предыдущую) ' +
                    'дату.Запись не произведена')


def main():
    while True:
        on_date = datetime.now().date() + timedelta(days=1)
        try:
            result = get_api_answer(on_date)
            if result:
                course = result["_value_1"]["_value_1"]
                load_to_database(course, on_date)
        except Exception as error:
            message = 'Вызов сервиса завершен с ошибкой: ' + str(error)
            logger.error(message)
        finally:
            time.sleep(RETRY_PERIOD)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format=('%(asctime)s [%(levelname)s] - '
                                '(%(filename)s).%(funcName)s:'
                                '%(lineno)d - %(message)s'
                                ),
                        handlers=[
                            logging.FileHandler(f'{BASE_DIR}/output.log'),
                            logging.StreamHandler(sys.stdout)]
                        )
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(stdout)
    logger.addHandler(handler)
    formatter = logging.Formatter('%(asctime)s - '
                                  '%(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    main()
