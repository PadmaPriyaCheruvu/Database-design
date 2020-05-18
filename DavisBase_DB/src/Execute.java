import java.io.File;
import java.io.RandomAccessFile;

public class Execute {
	
	private static RandomAccessFile davisbaseTablesCatalog;
	private static RandomAccessFile davisbaseColumnsCatalog;
	public static final int pageSize = 512;
	
	public static void show()
	{
			String[] columns = {"table_name"};
			String[] sample = new String[0];
			
			
			select("data//internalTables//davisbase_tables.tbl","davisbase_tables", columns, sample);
	}
	
	public static void createTable(String table, String[] col)
	{
			try{	
				
				File catalog = new File("data//"+DavisBase.currentDB+"//"+table);
				
				catalog.mkdir();
				RandomAccessFile createfile = new RandomAccessFile("data//"+DavisBase.currentDB+"//"+table+"//"+table+".tbl", "rw");
				createfile.setLength(pageSize);
				createfile.seek(0);
				createfile.writeByte(0x0D);
				createfile.close();
				
				
				createfile = new RandomAccessFile("data//internalTables//davisbase_tables.tbl", "rw");
				int num = ReadTable.pages(createfile);
				int page = 1;
				for(int r = 1; r <= num; r++)
				{
					int rightmost = Page.getRt(createfile, r);
					if(rightmost == 0)
						page = r;
				}
				int[] keys = Page.getKey(createfile, page);
				int l = keys[0];
				for(int i = 0; i < keys.length; i++)
					if(l < keys[i])
						l = keys[i];
				createfile.close();
				String[] values = {Integer.toString(l+1), DavisBase.currentDB+"."+table};
				System.out.println(table);
				insertInto("davisbase_tables", values);

				RandomAccessFile catalogfile = new RandomAccessFile("data//internalTables//davisbase_columns.tbl", "rw");
				Buffer buffer = new Buffer();
				String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
				String[] comparision = {};
				ReadTable.filter(catalogfile, comparision, columnName, buffer);
				l = buffer.content.size();

				for(int i = 0; i < col.length; i++){
					l = l + 1;
					String[] sub = col[i].split(" ");
					String n = "YES";
					if(sub.length > 2)
						n = "NO";
					String col_name = sub[0];
					String dt = sub[1].toUpperCase();
					String pos = Integer.toString(i+1);
					String[] v = {Integer.toString(l), DavisBase.currentDB+"."+table, col_name, dt, pos, n};
					insertInto("davisbase_columns", v);
				}
				catalogfile.close();
				createfile.close();
			}catch(Exception e){
				System.out.println("Error at createTable");
				e.printStackTrace();
			}
	}
	
	public static void insertInto(RandomAccessFile file, String table, String[] values)
	{
			String[] dt = ReadTable.getDataType(table);
			String[] nullable = ReadTable.getNullable(table);

			for(int i = 0; i < nullable.length; i++)
				if(values[i].equals("null") && nullable[i].equals("NO")){
					System.out.println("NULL constraint violation");
					
					return;
				}


			int sk = new Integer(values[0]);
			int pageno = ReadTable.searchKey(file, sk);
			if(pageno != 0)
				if(Page.hasKey(file, pageno, sk))
				{
					System.out.println("Uniqueness constraint violation");
					System.out.println();
					return;
				}
			if(pageno == 0)
				pageno = 1;


			byte[] bt = new byte[dt.length-1];
			short plSize = (short)(ReadTable.calPayloadSize(table, values, bt));
			int cellSize = plSize + 6;
			int offset = Page.checkLeafSpace(file, pageno, cellSize);

			if(offset != -1)
			{
				Page.insertLfCell(file, pageno, offset, plSize, sk, bt, values,table);
				
			}
			else
			{
				Page.splitlf(file, pageno);
				insertInto(file, table, values);
			}
			
	}
	public static void insertInto(String table, String[] values)
	{
			try
			{
				RandomAccessFile insertfile = new RandomAccessFile("data//internalTables//"+table+".tbl", "rw");
				insertInto(insertfile, table, values);
				insertfile.close();

			}
			catch(Exception e)
			{
				System.out.println("Error while inserting the data");
				e.printStackTrace();
			}
	}
	
	public static void select(String file, String table, String[] cols, String[] comp)
	{
			try
			{
				Buffer buffer = new Buffer();
				String[] colName = ReadTable.getColName(table);
				String[] dt = ReadTable.getDataType(table);

				RandomAccessFile rFile = new RandomAccessFile(file, "rw");
				ReadTable.filter(rFile, comp, colName, dt, buffer);
				buffer.display(cols);
				rFile.close();
			}
			catch(Exception e)
			{
				System.out.println("Error at select");
				System.out.println(e);
			}
	}
	
	public static void drop(String table,String db) 
	{
			try{
				
				RandomAccessFile dropfile = new RandomAccessFile("data//internalTables//davisbase_tables.tbl", "rw");
				int num = ReadTable.pages(dropfile);
				for(int page = 1; page <= num; page ++)
				{
					dropfile.seek((page-1)*pageSize);
					byte kind= dropfile.readByte();
					if(kind== 0x05)
						continue;
					else{
						short[] cells = Page.getCellArr(dropfile, page);
						int i = 0;
						for(int j = 0; j < cells.length; j++){
							long location = Page.getCellLoc(dropfile, page, j);
							String[] pl = ReadTable.retrievePayload(dropfile, location);
							String tb = pl[1];
							if(!tb.equals(DavisBase.currentDB+"."+table)){
								Page.setCellOffset(dropfile, page, i, cells[j]);
								i++;
							}
						}
						Page.setCellNum(dropfile, page, (byte)i);
					}
				}

				
				dropfile = new RandomAccessFile("data//internalTables//davisbase_columns.tbl", "rw");
				num = ReadTable.pages(dropfile);
				for(int page = 1; page <= num; page ++){
					dropfile.seek((page-1)*pageSize);
					byte kind = dropfile.readByte();
					if(kind == 0x05)
						continue;
					else{
						short[] cells = Page.getCellArr(dropfile, page);
						int i = 0;
						for(int j = 0; j < cells.length; j++){
							long location = Page.getCellLoc(dropfile, page, j);
							String[] pl = ReadTable.retrievePayload(dropfile, location);
							String tb = pl[1];
							if(!tb.equals(DavisBase.currentDB+"."+table))
							{
								Page.setCellOffset(dropfile, page, i, cells[j]);
								i++;
							}
						}
						Page.setCellNum(dropfile, page, (byte)i);
					}
				}
				dropfile.close();
				File drop = new File("data//"+db+"//"+table);
				String[] lf = drop.list();
				for(String f:lf){
					File dropFile = new File("data//"+db+"//"+table,f);
					dropFile.delete();
				}
				drop = new File("data//"+db, table); 
				drop.delete();
			}
			catch(Exception e)
			{
				System.out.println("error during drop");
				System.out.println(e);
			}

	}

}
