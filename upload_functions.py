
#update_sourroundings: update calendar data, store_ids, item_ids 

def update_surrounding(salesData, calendarData, engine, pd):
####################################### sale item categories  #######################################
        categories = salesData[['item_id','dept_id','cat_id']].drop_duplicates()

        categories.rename(columns = {'item_id':'item_id',
                                'dept_id':'department_id',
                                'cat_id':'category'}).to_sql('t_item_temporary', schema  = 'walmart',
                                                                        con = engine, 
                                                                        chunksize= 100,
                                                                        if_exists = 'replace', #if table already exits
                                                                        method = 'multi',
                                                                        index = False)
        with engine.begin() as cnx:
                insert_sql = 'INSERT INTO walmart.t_item (SELECT * FROM walmart.t_item_temporary) ON CONFLICT (item_id, valid_to) DO NOTHING'
                cnx.execute(insert_sql) 
                del_sql = 'DROP TABLE walmart.t_item_temporary'
                cnx.execute(del_sql) 

        del categories
                                

        ####################################### countries ######################################
        countries = salesData[['state_id']].drop_duplicates()

        #in terminal: wget https://raw.githubusercontent.com/jasonong/List-of-US-States/master/states.csv
        countries_all = pd.read_csv("data/states.csv")

        countries_all = countries_all[['Abbreviation','State']].rename(columns={'Abbreviation':'name_short',
                                                                                'State':'name_long'})

        countries_all = countries_all[countries_all['name_short'].isin(countries['state_id'].to_list())]

        countries_all['first_day_of_week_en'] = 'Sunday'

        countries_all.to_sql('t_country_temporary', schema  = 'walmart',
                                                                        con = engine, 
                                                                        chunksize= 100,
                                                                        if_exists = 'replace', #if table already exits
                                                                        method = 'multi',
                                                                        index = False)
        with engine.begin() as cnx:
                insert_sql = 'INSERT INTO walmart.t_country (SELECT * FROM walmart.t_country_temporary) ON CONFLICT (name_short, valid_to) DO NOTHING'
                cnx.execute(insert_sql) 
                del_sql = 'DROP TABLE walmart.t_country_temporary'
                cnx.execute(del_sql) 

        #del countries 


        ####################################### stores ######################################
        stores = salesData[['store_id','state_id']].drop_duplicates()


        stores.rename(columns = {'store_id':'id',                      
                                'state_id':'country_id'}).to_sql('t_store_temporary', schema  = 'walmart',
                                                                        con = engine, 
                                                                        chunksize= 100,
                                                                        if_exists = 'replace', #if table already exits
                                                                        method = 'multi',
                                                                        index = False)
        with engine.begin() as cnx:
                insert_sql = 'INSERT INTO walmart.t_store (SELECT * FROM walmart.t_store_temporary) ON CONFLICT (id, valid_to) DO NOTHING'
                cnx.execute(insert_sql) 
                del_sql = 'DROP TABLE walmart.t_store_temporary'
                cnx.execute(del_sql) 

        del stores


        ####################################### event handling ######################################
        #special events:
        events_long = calendarData[['date', 'event_name_1','event_type_1']].rename(columns = {'event_name_1': 'event_name',
                                                                                                'event_type_1': 'event_type'}
                                                                                                ).append(
                calendarData[['date', 'event_name_2', 'event_type_2']].rename(columns = {'event_name_2': 'event_name',
                                                                                                'event_type_2': 'event_type'}
                                                                                                ) #end of rename 
                                                                                                ) #end of append

        events_long = events_long.dropna()

        #create state holidays for each country
        events_long = events_long.merge(countries['state_id'], how = "cross")[[
                'date','state_id','event_name','event_type'
        ]].rename(columns = {'state_id' : 'state_short'})



        #add snaps to event table
        snaps_long = pd.melt(calendarData[['date','d', 'snap_CA', 'snap_TX', 'snap_WI']],
                id_vars = ['date','d'],
                var_name = 'country',
                value_name = 'amount')


        snaps_long['country'] = snaps_long['country'].str.split("_", n = 1, expand = False).str[1]       
        snaps_long = snaps_long[snaps_long.amount == 1]

        snaps_long = snaps_long[['date','country']].rename(
                                        columns = {'country' : 'state_short'})

        snaps_long['event_name'] = 'SNAP'
        snaps_long['event_type'] = 'State'

        events_long = events_long.append(snaps_long)


        events_long['date'] = pd.to_datetime(events_long['date'])

        #rename to match table definition
        events_long.rename(columns = {'store_id':'id',
                                        'event_name':'name',
                                        'event_type':'category',
                                        'state_short':'country'}).to_sql('t_event_long_temporary', schema  = 'walmart',
                                                                        con = engine, 
                                                                        chunksize= 100,
                                                                        if_exists = 'replace', #if table already exits
                                                                        method = 'multi',
                                                                        index = False)
        with engine.begin() as cnx:
                insert_sql = 'INSERT INTO walmart.t_event_long ("date", "name", category, country) (SELECT "date", "name", category, country FROM walmart.t_event_long_temporary) ON CONFLICT ("date","name","country") DO NOTHING'
                cnx.execute(insert_sql) 
                del_sql = 'DROP TABLE walmart.t_event_long_temporary'
                cnx.execute(del_sql)


         ####################################### calendar data ######################################
                
        #table defintion: date|year|month|day_of_month|day_of_week_walmart|day_of_year|week_of_year|
        calendarData['date'] = pd.to_datetime(calendarData['date'])
        calendarData['day_of_month'] = calendarData.date.dt.day        
        calendarData = calendarData.rename(columns={'wday':'day_of_week_walmart'})
        calendarData['day_of_year'] = calendarData.date.dt.dayofyear       
        calendarData['week_of_year'] = calendarData.date.dt.week    
        
        
        calendarData[['date','year','month','day_of_month','day_of_week_walmart','day_of_year','week_of_year']].to_sql('t_calendar_temporary', schema  = 'walmart',
                                                                                                                        con = engine, 
                                                                                                                        chunksize= 100,
                                                                                                                        if_exists = 'replace', #if table already exits
                                                                                                                        method = 'multi',
                                                                                                                        index = False)
        with engine.begin() as cnx:
                insert_sql = 'INSERT INTO walmart.t_calendar (SELECT * FROM walmart.t_calendar_temporary) ON CONFLICT ("date") DO NOTHING'
                cnx.execute(insert_sql) 
                del_sql = 'DROP TABLE walmart.t_calendar_temporary'
                cnx.execute(del_sql)  

# end of update_surroundings data



def upload_salesData(salesData, calendarData, sellPrices, engine, pd):
        #melt data:
        salesData_long = pd.melt(salesData.drop(columns=['id', 'dept_id', 'cat_id', 'state_id']),
                                id_vars=['item_id','store_id'],
                                var_name='day',
                                value_name='amount')
   

        salesData_long = salesData_long.merge(calendarData[['d','date','wm_yr_wk']], 
                                                how = 'inner',
                                                left_on= 'day',
                                                right_on = 'd').drop(
                                                                columns=['day','d']
                                                                ) #end of drop 

        salesData_long = salesData_long.merge(sellPrices,
                                                how = 'left',
                                                on = ['item_id','store_id','wm_yr_wk'],
                                                ).drop(
                                                        columns='wm_yr_wk'
                                                ) #end of drop 

        salesData_long['date'] = pd.to_datetime(salesData_long['date'])

        salesData_long.rename(columns = {'item_id':'item_id',
                        'store_id':'store_id',
                        'amount':'cnt',
                        'sell_price':'price_per_unit'}).to_sql('t_sale_temporary', schema  = 'walmart',
                                                                con = engine, 
                                                                chunksize= 1000,
                                                                if_exists = 'replace', #if table already exits
                                                                method = 'multi',
                                                                index = False)
        with engine.begin() as cnx:
                insert_sql = 'INSERT INTO walmart.t_sale ("date", store_id, item_id, "cnt", price_per_unit) \
                                (SELECT "date", store_id, item_id, "cnt", price_per_unit FROM walmart.t_sale_temporary) ON CONFLICT ("date", store_id, item_id) DO NOTHING'
                cnx.execute(insert_sql) 
                del_sql = 'DROP TABLE walmart.t_sale_temporary'
                cnx.execute(del_sql) 
      
# end of upload_salesData function


