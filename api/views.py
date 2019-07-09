from flask import Flask, Blueprint, jsonify, request
from flask_cors import CORS, cross_origin
from datetime import time
from datetime import datetime
from datetime import date
import pandas
import pyodbc
import json

main = Blueprint('main',__name__)
CORS(main)

@main.route('/add_movie',methods=['POST'])
def add_movie():

    return 'Done', 201

@main.route('/rates')
def rates():
    conn = pyodbc.connect('Driver=SQL Server;'
                      'Server=D1V-SQLDEV01;'
                      'Database=ResearchData;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    df = pandas.read_sql_query("select a.date as 'Date',avg(a.rate) as 'Capital One',b.rate as 'Goldman Sachs Bank USA',c.rate as 'Citi',d.rate as 'Synchrony Bank',e.rate as 'American Express National Bank',f.rate as 'Sallie Mae Bank',g.rate as 'Discover Bank',h.rate as 'Ally Bank' from rates a left join (select date as 'Date',bank as 'Bank',avg(rate) as 'Rate' from rates where bank like ('%Goldman%') and type = 'Savings'  group by date, bank) b on b.Date = a.date left join (select date as 'Date',bank as 'Bank',avg(rate) as 'Rate' from rates where  bank like ('Citi') and type = 'Savings'  group by date, bank) c on c.Date = a.date left join (select date as 'Date',bank as 'Bank',avg(rate) as 'Rate' from rates where  bank like ('%Synchrony%') and type = 'Savings'  group by date, bank) d on d.Date = a.date left join (select date as 'Date',bank as 'Bank',avg(rate) as 'Rate' from rates where bank like ('%American Express%') and type = 'Savings'  group by date, bank) e on e.Date = a.date left join (select date as 'Date',bank as 'Bank',avg(rate) as 'Rate' from rates where bank like ('%Sallie%') and type = 'Savings'  group by date, bank) f on f.Date = a.date left join (select date as 'Date',bank as 'Bank',avg(rate) as 'Rate' from rates where bank like ('%Discover %') and type = 'Savings'  group by date, bank) g on g.Date = a.date left join (select date as 'Date',bank as 'Bank',avg(rate) as 'Rate' from rates where bank like ('%Ally %') and type = 'Savings'  group by date, bank) h on h.Date = a.date where a.bank like ('%Capital One%') and type = 'Savings'  group by a.date,b.rate,c.rate,d.rate,e.rate,f.rate,g.rate,h.rate order by a.date asc",conn) 
    conn.close()
    movies = []
    data= df.to_json(orient='records')
    movies = [i for i in json.loads(data)]
    
    return jsonify({'data':movies})


@main.route('/jobs', methods=['GET','POST'])
def jobs():
    if request.method == 'POST':
        ticker = request.json['ticker']
        print(ticker)
        conn = pyodbc.connect('Driver=SQL Server;'
                        'Server=D1V-SQLDEV01;'
                        'Database=ResearchData;'
                        'Trusted_Connection=yes;')
        cursor = conn.cursor()
        df = pandas.read_sql_query("select Date,Ticker,count(*) as Postings from ResearchData.dbo.jobs where ticker = '"+ticker+"' group by Date,Ticker order by Ticker,Date asc",conn) 
        conn.close()
        data= df.to_json(orient='records')
        jobs = [i for i in json.loads(data)]
        
    elif request.method == 'GET' and request.args.get('prefix') is not None:
        ticker = request.args.get('prefix')
        print(ticker)
        conn = pyodbc.connect('Driver=SQL Server;'
                        'Server=D1V-SQLDEV01;'
                        'Database=ResearchData;'
                        'Trusted_Connection=yes;')
        cursor = conn.cursor()
        df = pandas.read_sql_query("select Date,Ticker,count(*) as Postings from ResearchData.dbo.jobs where ticker like ('"+ticker+"%') group by Date,Ticker order by Ticker,Date asc",conn) 
        conn.close()
        data= df.to_json(orient='records')
        jobs = [i for i in json.loads(data)]
    else:
        conn = pyodbc.connect('Driver=SQL Server;'
                        'Server=D1V-SQLDEV01;'
                        'Database=ResearchData;'
                        'Trusted_Connection=yes;')
        cursor = conn.cursor()
        df = pandas.read_sql_query("select Date,Ticker,count(*) as Postings from ResearchData.dbo.jobs group by Date,Ticker order by Ticker,Date asc",conn) 
        conn.close()
        data= df.to_json(orient='records')
        jobs = [i for i in json.loads(data)]
    print(jobs) 
    return jsonify({'data':jobs})


@main.route('/flights')
def flights():
    conn = pyodbc.connect('Driver=SQL Server;'
                      'Server=D1V-SQLDEV01;'
                      'Database=ResearchData;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    df = pandas.read_sql_query("select a.datechecked as Date, round(avg(a.price),0) as 'Main Cabin Business Average', b.pcba as 'Premium Cabin Business Average', c.mcla as 'Main Cabin Leisure Average', d.pcla as 'Premium Cabin Leisure Average' from ResearchData.dbo.airlines a  JOIN   (select datechecked Date, round(avg(price),0) as 'pcba' from ResearchData.dbo.airlines where cabintype = 'Premium' and flighttype = 'Close-in' group by datechecked) b  on b.Date = a.datechecked  JOIN   (select datechecked Date, round(avg(price),0) as 'mcla' from ResearchData.dbo.airlines where cabintype = 'Main Cabin' and flighttype = 'Leisure' group by datechecked) c  on c.Date = a.datechecked  JOIN   (select datechecked Date, round(avg(price),0) as 'pcla' from ResearchData.dbo.airlines where cabintype = 'Premium' and flighttype = 'Leisure' group by datechecked) d  on d.Date = a.datechecked  where a.cabintype = 'Main Cabin' and a.flighttype = 'Close-in' group by a.datechecked, b.pcba, c.mcla, d.pcla  order by Date asc",conn) 
    conn.close()
    data= df.to_json(orient='records')
    flights = [i for i in json.loads(data)]
    
    return jsonify({'data':flights})

@main.route('/loads')
def loads():
    conn = pyodbc.connect('Driver=SQL Server;'
                      'Server=D1V-SQLDEV01;'
                      'Database=ResearchData;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    df = pandas.read_sql_query("select a.date as 'Date',source as Source, count(a.drivingMiles) as Loads,tripEquipment as 'Equipment' from ResearchData.dbo.loads a group by a.date, a.tripEquipment,a.source order by a.date asc",conn) 
    conn.close()
    data= df.to_json(orient='records')
    loads = [i for i in json.loads(data)]
    
    return jsonify({'data':loads})

@main.route('/deposits')
def deposits():
    conn = pyodbc.connect('Driver=SQL Server;'
                      'Server=D1V-SQLDEV01;'
                      'Database=ResearchData;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    df = pandas.read_sql_query("select * from rates where date = cast(getdate() as date)",conn) 
    conn.close()
    data = df.to_json(orient='records')
    deposits = [i for i in json.loads(data)]
    
    return jsonify({'data':deposits})

@main.route('/rigcounts')
def rigcounts():
    conn = pyodbc.connect('Driver=SQL Server;'
                      'Server=D1V-SQLDEV01;'
                      'Database=ResearchData;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    df = pandas.read_sql_query("select state,date,count(*) as counts from rig_count a right join (select max(date) as 'recent' from rig_count) b on b.recent = a.date group by state,date",conn) 
    conn.close()
    data = df.to_json(orient='records')
    rigcounts = [i for i in json.loads(data)]
    
    return jsonify({'data':rigcounts})


@main.route('/cars')
def cars():
    conn = pyodbc.connect('Driver=SQL Server;'
                      'Server=D1V-SQLDEV01;'
                      'Database=ResearchData;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    df = pandas.read_sql_query("select a.date as 'Date', sum(a.New) as 'Carmax New Listings', sum(a.sold) as 'Carmax Sold Listings', isnull(c.new,0) as 'Carvana New Listings', isnull(c.sold,0) as 'Carvana Sold Listings' from (select *, rank() over(partition by stockNumber order by date asc) as dayson, lead(stockNumber,1,0) over (order by stockNumber,date asc) as nextdaysn, lag(stockNumber,1,0) over (order by stockNumber,date asc) as previousdaysn, (case when(lag(stockNumber,1,0) over (order by stockNumber,date asc) <> stockNumber and date <> '2019-06-03') Then 1 Else 0 End) as New, (case when(lead(stockNumber,1,0) over (order by stockNumber,date asc) <> stockNumber and date <> CAST(GETDATE() AS DATE)) Then 1 Else 0 End) as sold from ResearchData.dbo.carmax) a  left outer join (select date, sum(case when(yeststatus = 'Available' and status = 'Sold') Then 1 Else 0 End) as sold, sum(case when(yeststock <> stockNumber and date <> '2019-06-10') Then 1 Else 0 End) as new from (select *, lag(status,1,0) over(partition by stockNumber,source order by stockNumber,date asc) as yeststatus, lag(stockNumber,1,0) over(partition by stockNumber,source order by stockNumber,date asc) as yeststock from carmax where source = 'Carvana') b group by date) c on c.date = a.date where source = 'Carmax' group by  a.date, c.sold, c.new order by date asc",conn) 
    conn.close()
    data = df.to_json(orient='records')
    cars = [i for i in json.loads(data)]
    
    return jsonify({'data':cars})

@main.route('/login', methods=['POST'])
def login():
    user = request.form.get('email')
    password = request.form.get('password')
    conn = pyodbc.connect('Driver=SQL Server;'
                      'Server=D1V-SQLDEV01;'
                      'Database=ResearchData;'
                      'Trusted_Connection=yes;')
        
    tablename = '"user"'
    query = "Select * from dbo."+tablename+" where username='"+user+"' and password='"+password+"'"
    df = pandas.io.sql.read_sql(query, conn) 

    if len(df.username) == 1:
        session['logged_in'] = True
        session['user'] = request.form['user']
    else:
        session['user'] = 0
    return jsonify({'data':session['user']})


@main.route('/redfin')
def redfin():
    conn = pyodbc.connect('Driver=SQL Server;'
                      'Server=D1V-SQLDEV01;'
                      'Database=ResearchData;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    df = pandas.read_sql_query("select b.Date, count(distinct b.Req) as Postings, sum(case when flag = 0 and b.Date <> CAST(GETDATE() AS DATE) Then 1 Else 0 End) as 'Filled Today', sum(sum(case when flag = 0 and b.Date <> CAST(GETDATE() AS DATE) Then 1 Else 0 End)) over(order by Date) as 'Total Filled' from (select a.Date, a.Req, Lead(left(a.Req,5),1,0) over (partition by a.Req order by a.Date asc) as 'flag' from (select * from jobs where ticker = 'RDFN' and Req Is not null and Req not In('','/care') and Req not LIKE '/care%' and Department = 'real-estate' and Title LIKE ('Real Estate Agent%') OR Title IN ('Agent') or Title LIKE ('Senior Agent%') or Title LIKE ('Lead Agent%') or Title LIKE ('Listing Agent%') or Title LIKE ('Real Estate Listing Agent%') or Title LIKE ('Bilingual Real Estate Agent%') or Title LIKE ('REO Agent%')) a group by a.date,a.Req) b group by b.Date",conn) 
    conn.close()
    print(df)
    data = df.to_json(orient='records')
    redfinjobs = [i for i in json.loads(data)]
    
    return jsonify({'data':redfinjobs})

@main.route('/bids')
def bids():
    conn = pyodbc.connect('Driver=SQL Server;'
                      'Server=D1V-SQLDEV01;'
                      'Database=ResearchData;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    df = pandas.read_sql_query("select a.Year, a.Month,LTM/LTMLY -1 as 'NC LTM YOY', c.ltmyoy as 'SC LTM YOY', e.ltmyoy as 'GA LTM YOY', g.ltmyoy as 'TN LTM YOY', i.ltmyoy as 'LA LTM YOY', k.ltmyoy as 'MO LTM YOY', m.ltmyoy as 'KY LTM YOY', o.ltmyoy as 'VA LTM YOY' from (select year(date) as Year,month(date) as Month,sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)) as Bids, (sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),1,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),2,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),3,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),4,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),5,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),6,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),7,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),8,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),9,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),10,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),11,0) over (order by year(date)))/12 as 'LTM', (lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),12,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),13,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),14,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),15,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),16,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),17,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),18,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),19,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),20,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),21,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),22,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),23,0) over (order by year(date)))/12 as 'LTMLY' from bids where state = 'NC' group by year(date),month(date)) a  left join (select Year, Month,LTM/LTMLY -1 as 'ltmyoy' from (select year(date) as Year,month(date) as Month,sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)) as Bids, (sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),1,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),2,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),3,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),4,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),5,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),6,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),7,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),8,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),9,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),10,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),11,0) over (order by year(date)))/12 as 'LTM', (lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),12,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),13,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),14,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),15,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),16,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),17,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),18,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),19,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),20,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),21,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),22,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),23,0) over (order by year(date)))/12 as 'LTMLY' from bids where state = 'SC' group by year(date),month(date)) b where Year > 2014) c on c.Year = a.Year and c.Month = a.Month  left join (select Year, Month,LTM/LTMLY -1 as 'ltmyoy' from (select year(date) as Year,month(date) as Month,sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)) as Bids, (sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),1,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),2,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),3,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),4,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),5,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),6,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),7,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),8,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),9,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),10,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),11,0) over (order by year(date)))/12 as 'LTM', (lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),12,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),13,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),14,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),15,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),16,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),17,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),18,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),19,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),20,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),21,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),22,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),23,0) over (order by year(date)))/12 as 'LTMLY' from bids where state = 'GA' group by year(date),month(date)) d where Year > 2014) e on e.Year = a.Year and e.Month = a.Month  left join (select Year, Month,LTM/LTMLY -1 as 'ltmyoy' from (select year(date) as Year,month(date) as Month,sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)) as Bids, (sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),1,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),2,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),3,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),4,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),5,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),6,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),7,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),8,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),9,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),10,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),11,0) over (order by year(date)))/12 as 'LTM', (lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),12,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),13,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),14,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),15,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),16,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),17,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),18,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),19,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),20,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),21,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),22,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),23,0) over (order by year(date)))/12 as 'LTMLY' from bids where state = 'TN' group by year(date),month(date)) f where Year > 2014) g on g.Year = a.Year and g.Month = a.Month  left join (select Year, Month,LTM/LTMLY -1 as 'ltmyoy' from (select year(date) as Year,month(date) as Month,sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)) as Bids, (sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),1,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),2,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),3,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),4,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),5,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),6,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),7,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),8,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),9,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),10,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),11,0) over (order by year(date)))/12 as 'LTM', (lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),12,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),13,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),14,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),15,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),16,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),17,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),18,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),19,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),20,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),21,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),22,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),23,0) over (order by year(date)))/12 as 'LTMLY' from bids where state = 'LA' group by year(date),month(date)) h where Year > 2014) i on i.Year = a.Year and i.Month = a.Month  left join (select Year, Month,LTM/LTMLY -1 as 'ltmyoy' from (select year(date) as Year,month(date) as Month,sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)) as Bids, (sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),1,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),2,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),3,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),4,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),5,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),6,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),7,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),8,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),9,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),10,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),11,0) over (order by year(date)))/12 as 'LTM', (lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),12,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),13,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),14,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),15,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),16,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),17,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),18,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),19,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),20,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),21,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),22,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),23,0) over (order by year(date)))/12 as 'LTMLY' from bids where state = 'MO' group by year(date),month(date)) j where Year > 2014) k on k.Year = a.Year and k.Month = a.Month  left join (select Year, Month,LTM/LTMLY -1 as 'ltmyoy' from (select year(date) as Year,month(date) as Month,sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)) as Bids, (sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),1,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),2,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),3,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),4,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),5,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),6,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),7,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),8,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),9,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),10,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),11,0) over (order by year(date)))/12 as 'LTM', (lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),12,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),13,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),14,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),15,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),16,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),17,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),18,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),19,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),20,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),21,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),22,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),23,0) over (order by year(date)))/12 as 'LTMLY' from bids where state = 'KY' group by year(date),month(date)) l where Year > 2014) m on m.Year = a.Year and m.Month = a.Month  left join (select Year, Month,LTM/LTMLY -1 as 'ltmyoy' from (select year(date) as Year,month(date) as Month,sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)) as Bids, (sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),1,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),2,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),3,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),4,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),5,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),6,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),7,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),8,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),9,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),10,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),11,0) over (order by year(date)))/12 as 'LTM', (lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),12,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),13,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),14,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),15,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),16,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),17,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),18,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),19,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),20,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),21,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),22,0) over (order by year(date))+lag(sum(cast(replace(replace(Bid_Amount,'$',''),',','') as float)),23,0) over (order by year(date)))/12 as 'LTMLY' from bids where state = 'VA' group by year(date),month(date)) n where Year > 2014) o on o.Year = a.Year and o.Month = a.Month  where a.Year > 2014",conn) 
    conn.close()
    print(df)
    data = df.to_json(orient='records')
    bids = [i for i in json.loads(data)]
    
    return jsonify({'data':bids})