import java.io.File;
import java.util.Scanner;

public class DavisBase {
static Scanner scanner = new Scanner(System.in).useDelimiter(";");
static String currentDB = "myData";

	public static void main(String[] args) {
		createDavisBase();
		System.out.println("DavisBase");
		System.out.println(
				"---------------------------------------------------------------------");
		
		String query = "";
		//Loop until user input exists
		while (!query.equals("exit")) 
		{
			System.out.print("davisql> ");
			query = scanner.next().replace("\n", " ").replace("\r", "");
			query = query.trim().toLowerCase();
			ParseQuery.parse(query);
		}
		System.out.println("Exiting...");
	}
	public static void createDavisBase() 
	{
		try {
			File data = new File("data");
			if (!data.exists()) {
				data.mkdir();
			}
			File catalog = new File("data//internalTables");
			if (catalog.mkdir()) {
				Initializer.initialize();
			}
			else {

				boolean catalog1 = false;
				boolean catalog2 =false;
				String catalog_columns = "davisbase_columns.tbl";
				String catalog_tables = "davisbase_tables.tbl";
				String[] catalogList = catalog.list();

				for (int i = 0; i < catalogList.length; i++) {
					if (catalogList[i].equals(catalog_columns))
						catalog1 = true;
				}
				if (!catalog1) {
					System.out.println();
					Initializer.initialize();
				}
				
				for (int i = 0; i < catalogList.length; i++) {
					if (catalogList[i].equals(catalog_tables))
						catalog2 = true;
				}
				if (!catalog2) {
					System.out.println();
					Initializer.initialize();
				}
			}
		} catch (SecurityException se) {
			System.out.println("Application catalog not created " + se);

		}

	}
	
	public static boolean tableExist(String table) {
		boolean tableexists = false;
		
		try {
			File userdatafolder = new File("data//myData");
			if (userdatafolder.mkdir()) {
				
			}
			String[] usertables;
			usertables = userdatafolder.list();
			for (int i = 0; i < usertables.length; i++) {
				if (usertables[i].equals(table))
					return true;
			}
		} catch (SecurityException se) {
			System.out.println("Unable to create data container directory" + se);
		}

		return tableexists;
	}
	
	public static String[] equationParser(String equ) 
	{
		String cmp[] = new String[3];
		String temp[] = new String[2];
		if (equ.contains("=")) {
			temp = equ.split("=");
			cmp[0] = temp[0].trim();
			cmp[1] = "=";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains(">")) {
			temp = equ.split(">");
			cmp[0] = temp[0].trim();
			cmp[1] = ">";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains("<")) {
			temp = equ.split("<");
			cmp[0] = temp[0].trim();
			cmp[1] = "<";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains(">=")) {
			temp = equ.split(">=");
			cmp[0] = temp[0].trim();
			cmp[1] = ">=";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains("<=")) {
			temp = equ.split("<=");
			cmp[0] = temp[0].trim();
			cmp[1] = "<=";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains("<>")) {
			temp = equ.split("<>");
			cmp[0] = temp[0].trim();
			cmp[1] = "<>";
			cmp[2] = temp[1].trim();
		}

		return cmp;
	}
	
}
