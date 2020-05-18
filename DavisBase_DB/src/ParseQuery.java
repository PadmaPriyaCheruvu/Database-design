import java.io.RandomAccessFile;

public class ParseQuery {
	public static void parse(String query)
	{

		String[] substrings = query.split(" ");	
		String keyWord = substrings[0];
		
		if(keyWord.equals("create")) {
			createQuery(query);}
		else if(keyWord.equals("insert")){
			insertQuery(query);}
		else if(keyWord.equals("select")) {
			selectQuery(query);}
		else if(keyWord.equals("drop")) {
			dropQuery(query);}
		else if(keyWord.equals("show"))		{
			String tables = substrings[1];
			System.out.println();
			if(tables.equals("tables")){
				Execute.show();
			}
			else{
				System.out.println("\nCorrect usage - \n show tables;");
			}
		}
		else if(keyWord.equals("exit"))
		{
			System.out.println();
		}
		else if(keyWord.equals("help"))
		{
			System.out.println("CREATE COMMAND \nCREATE TABLE table_name (\r\n" + 
					"    column1 datatype,\r\n" + 
					"    column2 datatype,\r\n" + 
					"    column3 datatype,\r\n" + 
					"   ....\r\n" + 
					"); \n\nExample - create table employee (emp_id int primary key,name char, salary int);\r\n\n====================================\n" + 
					"INSERT COMMAND \nINSERT INTO table_name (column1, column2, column3, ...)\r\n" + 
					"VALUES (value1, value2, value3, ...); \n\nExample - insert into employee (emp_id,name,salary) values (1,John,100000); \n\n====================================\nSELECT COMMAND \nSELECT column1, column2, ...\r\n" + 
					"FROM table_name WHERE <Condition>;\n\nExample - select * from employee\r\n" + 
					"where emp_id = 1; \n====================================\n DROP COMMAND \n DROP TABLE TABLE_NAME \n\nExample - Drop table employee;\n====================================\n"
					);
		}
		else
		{
			System.out.println("\nInvalid query. Use 'help;' for the list of valid commands.\n");
		}
	}
	public static void createQuery(String query){
		String[] subs = query.split(" ");
		try {
		if(subs[1].equals("table")){
				String tablename = subs[2];
				
				String[] temp = query.split(tablename);
	
				String temp2 = temp[1].trim();
				int length=temp2.length();
				if(temp2.charAt(0)=='(' && temp2.charAt(length-1)==')'){
					String[] colnames = temp2.substring(1, temp2.length() - 1).split(",");
					for (int i = 0; i < colnames.length; i++){
						colnames[i] = colnames[i].trim();
					}
					if (!DavisBase.tableExist(tablename)) 
					{
						Execute.createTable(tablename, colnames);
						System.out.println("Table "+tablename+" created successfully.\n");
						
												
					}
					else{
						System.out.println("Table " + tablename + " already exists.\n");
					}
				}
				else{
					System.out.println("\nInvalid query. Use 'help;' for the list of valid commands.\n");
				}
		}
			
		else{
			System.out.println("Correct usage -\n CREATE TABLE table_name (\r\n" + 
					"    column1 datatype,\r\n" + 
					"    column2 datatype,\r\n" + 
					"    column3 datatype,\r\n" + 
					"   ....\r\n" + 
					");");				
		}
		}
		catch(Exception e)
		{
			System.out.println("Correct usage -\n CREATE TABLE table_name (\r\n" + 
					"    column1 datatype,\r\n" + 
					"    column2 datatype,\r\n" + 
					"    column3 datatype,\r\n" + 
					"   ....\r\n" + 
					");");
		}
	}

	public static void insertQuery(String query){
		String[] subs = query.split(" ");

		String insert_table = subs[2];
		String insert_vals1 = query.split("values")[1].trim();
		insert_vals1 = insert_vals1.substring(1, insert_vals1.length() - 1);
		String[] insert_vals2 = insert_vals1.split(",");
		for (int i = 0; i < insert_vals2.length; i++)
			insert_vals2[i] = insert_vals2[i].trim();
		if (!DavisBase.tableExist(insert_table)) {
			System.out.println("Table " + insert_table + " doesn't exist.\n");
			return;
		}
		RandomAccessFile insertfile;
		try {
			insertfile = new RandomAccessFile("data//myData//"+insert_table+"//"+insert_table+".tbl", "rw");
			Execute.insertInto(insertfile,insert_table, insert_vals2);
			insertfile.close();
		} catch (Exception e)
		{
			
			e.printStackTrace();
		}

	}
	
	public static void selectQuery(String query){
		try
		{
		String[] subs = query.split(" ");
		String[] condition;
		String[] select_column;
		String[] temp = query.split("where");
		String[] temp1 = temp[0].split("from");
		String table = temp1[1].trim();
		String columns = temp1[0].replace("select", "").trim();
		
		if(table.equals("davisbase_tables") || table.equals("davisbase_columns"))
		{
			if (columns.contains("*")) 
			{
				select_column = new String[1];
				select_column[0] = "*";
			} 
			else {
			
				select_column = columns.split(",");
				for (int i = 0; i < select_column.length; i++)
					select_column[i] = select_column[i].trim();
			}
			if (temp.length > 1) {
				String filter = temp[1].trim();
				condition = DavisBase.equationParser(filter);
			} else {
				condition = new String[0];
			}
			if(table.equals("davisbase_tables")){
				Execute.select("data//internalTables//davisbase_tables.tbl", table, select_column, condition);
				System.out.println();
				return;
			}
			else{
				Execute.select("data//internalTables//davisbase_columns.tbl", table, select_column, condition);
				System.out.println();
				return;
			}
		}

		else{
			if(!DavisBase.tableExist(table)) {
	
				System.out.println("Table " + table + " doesn't exist.");
				System.out.println("Please enter the correct table name.\n");
				
				return;
			}
			
			
		}

		if (temp.length > 1) 
		{
			String filter = temp[1].trim();
			condition = DavisBase.equationParser(filter);
		} 
		else {
			condition = new String[0];
		}

		if (columns.contains("*")) {
			select_column = new String[1];
			select_column[0] = "*";
		} else {
			select_column = columns.split(",");
			for (int i = 0; i < select_column.length; i++)
				select_column[i] = select_column[i].trim();
		}
		
		Execute.select("data//myData//"+table+"//"+table+".tbl", table, select_column, condition);
		System.out.println();
		}
		catch(Exception e)
		{
			System.out.println("\nInvalid query. Use 'help;' for the list of valid commands.\n");
		}
}

	public static void dropQuery(String query){
		String[] subs = query.split(" ");

		if(subs[1].equals("table")){
			String dropTable = subs[2];
			if (DavisBase.tableExist(dropTable)) 
			{	
				Execute.drop(dropTable,"myData");
				System.out.println("Table "+dropTable+" dropped successfully.\n");
				
			}
			else{
				System.out.println("Table " + dropTable + " does not exist.\n");
				
				
			}
			
		}
		else{
			System.out.println("\nInvalid query. Use 'help;' for the list of valid commands.\n");
		}
		
}
	
}
