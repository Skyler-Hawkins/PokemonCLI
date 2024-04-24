
import psycopg2

# SOURCE CODE FOR POKEMON DB CLI
# MADE BY SKYLER HAWKINS
# Refer to manual for specific instructions on how to insert data


'''
THE ORDER OF THE CODE: 
1st: Database connection and dictionary setup
2nd: Helper functions to assist in operations used across all different operations
3rd: Main functions for each operation
4th: Main loop for the program, driving force of CLI
'''

# Database connection Here
# Skyler's password omitted for security purposes
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="Boofboofdoof",
    host="localhost"  # or the IP address of your database server
)
cur = conn.cursor()

# Now, create list of dictionary: Each tuple will contain the table name as the key and the column names of that table
Pokemon_dict = {'Pokemon': ['name', 'generationid', 'abilityName1','abilityName2','abilityName3', 'type1', 'type2', 'hp', 'attack', 'defense', 'sp_attack', 'sp_defense', 'speed', '*']}
Pokedex_dict = {'Pokedex': ['pokedexEntryNum', 'classfication', 'generation', '*']}
Abilities_dict = {'Abilities': ['abilityName', '*']}
Generation_dict = {'Generation': ['generationid', 'region', '*']}
Types_dict = {'Types': ['typeName', 'generation', '*']}
tableList = ['Pokemon', 'Pokedex', 'Ability', 'Generation', 'Types']
aggregate_functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', '*']


# Helper functions to assist in operations used across all different operations
def showQuery():       
    print("Query results from postgres:\n")
    try:
        # Fetch all rows
        rows = cur.fetchall()

        # Get column names from cursor description
        columns = [desc[0] for desc in cur.description]

        # Print the column names
        print(' | '.join(columns))

        # Print a separator
        print('-' * 50)

        # Iterate over the rows and print each one
        for row in rows:
            print(' | '.join(str(item) for item in row))
    except Exception as e:
        if e == "no results to fetch" or e == "no results to fetch ":
            print("completed")
        else:
            print(f"An error occurred: {e}")

def showTables():
    print("Tables in the database:")
    for table in tableList:
        print(table)

def showColumns(tableName):
    # uses above dictionaries to show all column names
    table_dict = globals()[tableName + '_dict']
    column_names = table_dict[tableName]
    print(f"Column names for {tableName}:")
    for i, column_name in enumerate(column_names, start=1):
        print(f"{i}: {column_name}")
        
def inputTables():
    showTables()
    print("enter 'exit' to stop adding tables")
    tableNameList = []
    while(True):
        tableName = input("Enter the table name: ")
        if tableName == 'exit':
            break
        # check if tableName is in the list
        if tableName not in tableList:
            while(True):
                tableName = input("Table name not found. Please enter a valid table name: ")
                if tableName in tableList:
                    break
        tableNameList.append(tableName)
    return tableNameList

def inputSingleTable():
    showTables()
    tableName = input("Enter the table name: ")
    # check if tableName is in the list 
    if tableName not in tableList:
        while(True):
            tableName = input("Table name not found. Please enter a valid table name: ")
            if tableName in tableList:
                break
    return tableName

def getColumnNamesInsertion(tableName):
    showColumns(tableName)
    columnNames = input("Enter the column numbers separated by commas (example: 1,2,4): ")
    columnNames = columnNames.split(',')
    columnNames = [int(column) for column in columnNames]
    for i in range(0, len(columnNames)):
        # just bc 0-indexed tables
        columnNames[i] = columnNames[i] - 1
    # now, need to do a loop for each entry in columnNames
    valueList = ""
    numVals = 0
    table_dict = globals()[tableName + '_dict']
    for column in columnNames:
        print(f"Enter the value for {table_dict[tableName][column]}: ")
        value = input()
        if numVals == 0: 
            valueList =  "'" + value + "'"
        else:
            valueList = valueList + ", '" + value + "'"
        numVals+=1
    columns = [table_dict[tableName][i] for i in columnNames]
    # Convert the list of column names to a comma-separated string
    columns_str = ', '.join(columns)
    return columns_str, valueList

def getColumnNames(tableName):
    showColumns(tableName)
    columnNames = input("Enter the column numbers separated by commas (example: 1,2,4): ")
    columnNames = columnNames.split(',')
    columnNames = [int(column) for column in columnNames]
    for i in range(0, len(columnNames)):
        # just bc 0-indexed tables
        columnNames[i] = columnNames[i] - 1
    # now, need to do a loop for each entry in columnNames

    table_dict = globals()[tableName + '_dict']
    columns = [table_dict[tableName][i] for i in columnNames]
    # Convert the list of column names to a comma-separated string
    columns_str = ', '.join(columns)
    return columns_str

def getColumnNamesAndTable(tableName):
    showColumns(tableName)
    columnNames = input("Enter the column numbers separated by commas (example: 1,2,4): ")
    columnNames = columnNames.split(',')
    columnNames = [int(column) for column in columnNames]
    for i in range(0, len(columnNames)):
        # just bc 0-indexed tables
        columnNames[i] = columnNames[i] - 1
    # now, need to do a loop for each entry in columnNames

    table_dict = globals()[tableName + '_dict']
    columns = [f"{tableName}.{table_dict[tableName][i]}" for i in columnNames]
    # Convert the list of column names to a comma-separated string
    columns_str = ', '.join(columns)
    return columns_str

def insertData(transact=False):
    # inserting data

    try:
        tableName = inputSingleTable()
        columns_str, valueList = getColumnNamesInsertion(tableName)

        # Modify the query to use the columns_str
        query = "INSERT INTO " + tableName + "(" + columns_str + ") VALUES(" + valueList + ");"
        print("Your insertion in SQL:\n")
        print(query)

        cur.execute(query)
        if transact == True:
           pass
        else:
            conn.commit()
        print("\nInsertion Completed Successfully!")
    except Exception as e:
        conn.rollback()
        print("An error occurred:", str(e))

def deleteData(transact=False):
    try:
        # deleting data
        # Need to get table to be deleted, then need to get the condition on what to be deleted
        # will need to break down input, so for example if name = 'pikachu', then we need to get the column name, the operator, and the value
        showTables()

        tableName = input("Enter the table name to be deleted from: ")
        # check if tableName is in
        if tableName not in tableList:
            while (True):
                tableName = input("Table name not found. Please enter a valid table name: ")
                if tableName in tableList:
                    break
        # next, need to break down query to figure out what the user's condition is


        # get columns
        column_str = getColumnNames(tableName)


        condition = input("Enter the condition for deletion (if no condition, leave empty, press enter): ")



        # so the goal is to just try the query, and echo any errors from it back to the user
        # instead of trying to parse the query, just try it and see if it works
        print("Your deletion in SQL:\n")
        if condition == "":
            query = "DELETE FROM " + tableName + ";"
        else:
            query = "DELETE FROM " + tableName + " WHERE " + condition + ";"
        print(query + "\n")
        cur.execute(query)
        if transact == True:
           pass
        else:
            conn.commit()
        print("\nDeletion Completed Successfully!")
    except Exception as e:
        conn.rollback()

        print("An error occurred:", str(e))

def updateData(transact=False):
    try:
        # updating data
        tableName = inputSingleTable()
        # next, need to to get columns and values to be updated
        setCondition = input("Enter the column and value to be updated (example: name = value1): ")
        # next, need to get the condition for the update
        whereCondition = input("Enter the condition for the update: ")

        print("Your update in SQL: \n")
        if whereCondition == "":
            query = "UPDATE " + tableName + "SET " + setCondition + ";"
        else:
            query = "UPDATE " + tableName + " SET " + setCondition + " WHERE " + whereCondition + ";"
        print(query + "\n")
        cur.execute(query)
        # showQuery()
        if transact == True:
           pass
        else:
            conn.commit()
        print("\nUpdate Completed Successfully!")
    except Exception as e:
        conn.rollback()
        print("An error occurred:", str(e))    

def searchData(transact=False):
    try:
        # searching data
        # Need to get the table name, then the columns to be queried, then the condition
        # 'basically just allowing the user to use sql in a more abstracted way'
        # first, get the table name
        tableName = inputSingleTable()

        showColumns(tableName)
        columns_str = getColumnNames(tableName)
        # next, get the condition
        condition = input("Enter the condition for the query: ")
 
        # Modify the query to use the columns_str
        if condition == "":
            query = "SELECT " + columns_str + " FROM " + tableName + ";"
        else:
            query = "SELECT " + columns_str +  " FROM " + tableName + " WHERE " + condition + ";"
        print("Your query in SQL: \n", query)

        cur.execute(query)

        showQuery()

        if transact == True:
           pass
        else:
            conn.commit()
        print("\nSearch Completed Successfully!")
    except Exception as e:
        conn.rollback()        
        print("An error occurred:", str(e))

def aggregateData(transact=False):
    try:  
        # aggregate functions
        fnchoice = input("Enter the aggregate function to be used (1: COUNT, 2: SUM, 3: AVG, 4: MIN, 5: MAX): ")
        fnchoice = int(fnchoice)
        if fnchoice not in range(1, 6):
            while(True):
                fnchoice = input("Invalid choice. Please enter a valid choice: ")
                fnchoice = int(fnchoice)
                if fnchoice in range(1, 6):
                    break
        fnchoice = aggregate_functions[fnchoice - 1]

        tableName = inputSingleTable()
        # next, get the columns to be aggregated
        table_dict = globals()[tableName + '_dict']
        column_names = table_dict[tableName]
        showColumns(tableName)

        columnNum = input("Enter the column number to be aggregated: ")
        columnNum = int(columnNum) - 1
        columnName = table_dict[tableName][columnNum]
        # making sure user picks a valid column for aggregation
        if columnName not in column_names:
            while(True):
                columnNum = input("Invalid choice. Please enter a valid choice: ")
                columnNum = int(columnNum)
                if columnName in column_names:
                    break
        

        # next, get the condition
        condition = input("Enter the condition for the query: ")
        # now, need to do a loop for each entry in columnNames
        if condition == "":
            query = "SELECT " + fnchoice + "(" + columnName + ") FROM " + tableName + ";"
        else:
            query = "SELECT " + fnchoice + "(" + columnName + ") FROM " + tableName + " WHERE " + condition + ";"
        print("Your aggregation query in SQL: \n", query)
        cur.execute(query)
        showQuery()
        if transact == True:
           pass
        else:
            conn.commit()
        print("\nAggregation Completed Successfully!")

    except Exception as e:
        cur.rollback()
        print("An error occurred:", str(e))

def sortData(transact=False):
    try:
        # sorting data
        # similar queries as before, except now add a sorting option
        tableName = inputSingleTable()

        # next, get the columns to be queried
        table_dict = globals()[tableName + '_dict']
        column_names = table_dict[tableName]
        columns_str = getColumnNames(tableName)

        # get the condition of the query
        condition = input("Enter the condition for the query: ")
        
        # get the columns to be sorted by
        # so here, I will prompt the user for a column, THEN a direction, in a loop. I will repeat that process until they input an exit condition (like 'exit')
        showColumns(tableName)
        sortColumns = []
        sortDirections = []
        # Here the user is inputting the column number to be sorted by, and the direction to sort by until they input 'done'
        while (True):
            print("Enter 'done' to stop entering sort columns")
            sortIndex = input("Enter the column number to be sorted by: \n")
            if len(sortColumns) == 0 and sortIndex == 'done':
                break
            if len(sortColumns) > 0:
                if sortIndex == 'done':
                    break            
            sortColumns.append(column_names[int(sortIndex) - 1])
            sortDirections.append(input("Enter the direction to sort by (ASC or DESC): \n"))
            if sortDirections[-1] == 'done':
                sortColumns.pop()
                sortDirections.pop()
                break
                
        
        # the sorted query:
        sortQuery = ""
        numVals = 0
        for i in range(0, len(sortColumns)):
            if numVals == 0:
                sortQuery = sortQuery + sortColumns[i] + " " + sortDirections[i]
            else:
                sortQuery += ", " +  sortColumns[i] + " " + sortDirections[i] 
            numVals +=1
        sortQuery += ";"
        if condition == "":
            query = "SELECT "+ columns_str + " FROM " + tableName + " ORDER BY " + sortQuery
        else:
            query = "SELECT "+ columns_str + " FROM " + tableName + " WHERE " + condition + " ORDER BY " + sortQuery
        
        print("Your sort query in SQL: \n", query)
        cur.execute(query)
        showQuery()
        if transact == True:
           pass
        else:
            conn.commit() 
        print("\nSorting Completed Successfully!")   
    except Exception as e:
        conn.rollback()
        print("An error occurred:", str(e))
    
def joinData(transact=False):
    try:
            
        # joins
        # for joins, it requires more user inputs, mostly related to the join type and the columns to be joined, and from what tables
        # first, get the join type
        joinType = input("Enter the join type (1: INNER JOIN, 2: LEFT JOIN, 3: RIGHT JOIN, 4: FULL JOIN): ")
        joinType = int(joinType)
        if joinType not in range(1, 5):
            while(True):
                joinType = input("Invalid choice. Please enter a valid choice: ")
                joinType = int(joinType)
                if joinType in range(1, 5):
                    break
        joinTypes = ['INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN']
        joinIndex = joinType - 1
        joinType = joinTypes[joinIndex]

        # next, get the tables to be joined
        joinTableList = []
        showTables()
        while(True):
            print("Enter 'exit' to stop entering tables to join")
            joinTableList.append(input("Enter the table name to be joined: \n"))
            if joinTableList[-1] == 'exit':
                joinTableList.pop()
                break

        # need to get columns to join on
        outerColumnList = []
        for tableName in joinTableList:
            # showColumns(tableName)
            # here, we will have a list of strings, where each entry in the list is the columns specified from each list.... then will be formatted as (tableName.columnName) in the query 
            columns_str = getColumnNamesAndTable(tableName)
            outerColumnList.append(columns_str)

        columnsQuery = " "
        numVals = 0
        for i in range(0, len(joinTableList)):
            if numVals == 0:
                columnsQuery = columnsQuery + outerColumnList[i] + " "
            else:
                columnsQuery = columnsQuery + ", " + outerColumnList[i] + " "
            numVals +=1

        joinCondition = input("Enter the condition for the join: ")

        query = "SELECT " + columnsQuery + " FROM " + joinTableList[0] + " " + joinType + " " + joinTableList[1] + " ON " + joinCondition + ";"
        print("Your join query in SQL: \n", query)
        cur.execute(query)
        showQuery()
        if transact == True:
           pass
        else:
            conn.commit()
        print("\nJoin Completed Successfully!")
    except Exception as e:
        conn.rollback()

        print("An error occurred:", str(e))            

def groupData(transact=False):
        # grouping
        # need to get table name first
        try:
            tableName = inputSingleTable()
            # next, get the columns to be queried
            table_dict = globals()[tableName + '_dict']
            column_str = getColumnNames(tableName)
            # get the condition of the query
            condition = input("Enter the condition for the query: ")

            # get the columns to be grouped by
            showColumns(tableName)
            groupQuery = []
            while (True):
                groupColumn = input("Enter the column number to be grouped by, enter 'done' to stop entering: \n")
                if groupColumn == 'done':
                    break
                groupColumn = int(groupColumn) - 1
                groupColumn = table_dict[tableName][groupColumn]
                groupQuery.append(groupColumn)


            print("If any columns were left out of the group by statement, you must input an aggregate function for them")            
            print("Enter 'done' if the above is not applicable to your query, otheriwse:")
            aggregateColumns = ""
            showColumns(tableName)
            while(True):
                print("Input a column to be aggregated: \n")
                columnAgg = input("")
                if columnAgg == 'done':
                    break
                columnAgg = int(columnAgg) - 1
                column_name = table_dict[tableName][columnAgg]
                print("Enter the aggregate function to be used (1: COUNT, 2: SUM, 3: AVG, 4: MIN, 5: MAX): ")
                fnchoice = input("")
                fnchoice = int(fnchoice)
                fnchoiceIndex = fnchoice - 1
                aggregateFunction = aggregate_functions[fnchoiceIndex]
                aggregateColumns += ", " + aggregateFunction + "('" + column_name + "')"



            

            numVals = 0
            groupStr = []
            for i in range(0, len(groupQuery)):
                if numVals == 0:
                    groupStr = groupQuery[i]
                else:
                    groupStr += ", " + groupQuery[i]
                numVals +=1
            # next, if the user left any other columns out of the group by statement

            # now need to format the aggregation 


            # the grouped query:
            if condition == "":
                query = "SELECT " + groupStr + aggregateColumns + " FROM " + tableName + " GROUP BY " + groupStr + ";"
            else:
                query = "SELECT " + column_str + " FROM " + tableName + " WHERE " + condition + " GROUP BY " + groupStr + ";"
            print("Your grouping query in SQL: \n", query)
            cur.execute(query)
            showQuery()
            if transact == True:
               pass
            else:
                conn.commit()
            print("\nGrouping Completed Successfully!")
        except Exception as e:
            conn.rollback()
            print("An error occurred:", str(e))

def subqueryData(transact=False):
        # subqueries
        # this is just a repeated query column... so I can basically make a loop that calls the 4th choice, then feeds that as an input condition to 
        # a second one, have a loop that does this until the user is done, then will display the final query

        # STRUCTURE: making a loop called 'subquery loop' that will prompt the user to make queries, where if they enter 'subquery' it will save their 
        # query as the outer query of the loop, and will then return to the start, asking the same questions. BUT if they enter anything other than 
        # subquery as their 'WHERE' condition, then it will take that query as the innermost query, and it will end the loop, giving the final query... 

        try:
            # searching data
            # Need to get the table name, then the columns to be queried, then the condition
            # 'basically just allowing the user to use sql in a more abstracted way
            # first, get the table name
            subquery = True
            totalQuery = ""
            numSubQueries = 0

            while(subquery == True):
                query = ""
                tableName = inputSingleTable()
                showColumns(tableName)
                table_dict = globals()[tableName + '_dict']

                columns_str = getColumnNames(tableName)

                # next, get the condition
                condition = input("Enter the condition for the query, if you wish to subqueries type 'subquery': ")
                if(condition != 'subquery'):
                    subquery = False
                    # Modify the query to use the columns_str
                    if condition == "":
                        query += "SELECT " + columns_str + " FROM " + tableName 
                    else:
                        query += "SELECT " + columns_str +  " FROM " + tableName + " WHERE " + condition 
                    print("your query: \n", query)
                    totalQuery += query
                else:
                    showColumns(tableName)
                    newcondition = input("enter the column number to be checking in the subquery: ")
                    newcondition = table_dict[tableName][int(newcondition) - 1]
                    query += "SELECT " + columns_str +  " FROM " + tableName + " WHERE "+ newcondition + " IN ("
                    totalQuery += query
                    numSubQueries += 1
            # at the end of the subqueries, must cap off with the number of subqueries worth of )
            totalQuery += ")"*numSubQueries + ";"
            print("FINISHED SUBQUERIES\n")
            print("Your total query in SQL: \n", totalQuery)

            cur.execute(totalQuery)
            showQuery()
            if transact == True:
                pass
            else:
                conn.commit()
            print("\nSubqueries Completed Successfully!")

        except Exception as e:
           conn.rollback()
           print("An error occurred:", str(e))
        

while True:

    choice = input('Welcome to Skylers Pokemon Database!\nNote*, when "*" is shown, it is to denote all columns\nbelow are your options\n'
             "1: Insert Data \n2: Delete Data \n3: Update Data \n4: Search Data \n5: Aggregate Functions \n"
            "6: Sorting Data \n7: Joins \n8: Grouping \n9: Subqueries \n"
            "10: Transactions\n11: Exit\n"
            "Enter your choice (1-11): ")
    # EACH condition leads to a seperate main function above, it is abstracted in this way so that implementing transactions is easier
    if choice == '1':
        insertData()        
    elif choice == '2':
        deleteData()
    elif choice == '3':
        updateData()
    elif choice == '4':
        searchData()
    elif choice == '5':
        aggregateData()
    elif choice == '6':
        sortData()
    elif choice == '7':
        joinData()
    elif choice == '8':
        groupData()
    elif choice == '9':
        subqueryData()
    elif choice == '10':
        # transactions
        # basically, just going to make a BEGIN loop that does begin, performs all queries and changes in a 
        # rollback loop, then if the user is satisfied, they can enter 'commit' to commit the changes
        # else they can enter 'rollback' to rollback the changes
        print("\nBEGINNING TRANSACTION\n")
        active = True
        while(active):
            print("")
            choice = input('Welcome to the Transaction Manager! below are your options\n'
                    "1: Insert Data \n2: Delete Data \n3: Update Data \n4: Search Data \n5: Aggregate Functions \n"
                    "6: Sorting Data \n7: Joins \n8: Grouping \n9: Subqueries \n"
                    "10: Commit \n 11: Rollback \n12: Exit\n"
                    "Enter your choice (1-11): ")
            if choice == '1':
                insertData(True)        
            elif choice == '2':
                deleteData(True)
            elif choice == '3':
                updateData(True)
            elif choice == '4':
                searchData(True)
            elif choice == '5':
                aggregateData()
            elif choice == '6':
                sortData(True)
            elif choice == '7':
                joinData(True)
            elif choice == '8':
                groupData(True)
            elif choice == '9':
                subqueryData(True)
            elif choice == '10':
                # committing all changes in this transaction
                conn.commit()
                print("\nCommitting all change in this transaction")
            elif choice == '11':
                # rolling back all changes in this transaction
                conn.rollback()
                print("\nRolling back all changes in this transaction")

            elif choice == '12':
                print("Exiting Transaction Manager")
                active = False
        pass
    elif choice == '11':
        # exit comment
        print("\nExiting the CLI, goodbye!")
        break

# CLOSING the connection to the DB
cur.close()
conn.close()