import java.io.File;
import java.io.RandomAccessFile;

public class Initializer {

	private static RandomAccessFile davisbaseTablesCatalog;
	private static RandomAccessFile davisbaseColumnsCatalog;
	public static final int pageSize = 512;
	
	public static void initialize() 
	{

		try {
				File catalog = new File("data//internalTables");
				
				String[] oldfiles;
				oldfiles = catalog.list();
				for (int i=0; i<oldfiles.length; i++) 
				{
					File anOldFile = new File(catalog, oldfiles[i]); 
					anOldFile.delete();
				}
			}
			catch (SecurityException se) 
			{
				System.out.println("Unable to create catalog directory :"+se);
				
			}

			try {
				davisbaseTablesCatalog = new RandomAccessFile("data//internalTables//davisbase_tables.tbl", "rw");
				davisbaseTablesCatalog.setLength(pageSize);
				davisbaseTablesCatalog.seek(0);
				davisbaseTablesCatalog.write(0x0D);
				davisbaseTablesCatalog.write(0x02);
				int[] offset=new int[2];
				int size1=24;
				int size2=25;
				offset[0]=pageSize-size1;
				offset[1]=offset[0]-size2;
				davisbaseTablesCatalog.writeShort(offset[1]);
				davisbaseTablesCatalog.writeInt(0);
				davisbaseTablesCatalog.writeInt(10);
				davisbaseTablesCatalog.writeShort(offset[1]);
				davisbaseTablesCatalog.writeShort(offset[0]);
				davisbaseTablesCatalog.seek(offset[0]);
				davisbaseTablesCatalog.writeShort(20);
				davisbaseTablesCatalog.writeInt(1); 
				davisbaseTablesCatalog.writeByte(1);
				davisbaseTablesCatalog.writeByte(28);
				davisbaseTablesCatalog.writeBytes("davisbase_tables");
				davisbaseTablesCatalog.seek(offset[1]);
				davisbaseTablesCatalog.writeShort(21);
				davisbaseTablesCatalog.writeInt(2); 
				davisbaseTablesCatalog.writeByte(1);
				davisbaseTablesCatalog.writeByte(29);
				davisbaseTablesCatalog.writeBytes("davisbase_columns");
			}
			catch (Exception e) 
			{
				System.out.println("Unable to create the database_tables file");
				System.out.println(e);
			}
			
			try 
			{
				davisbaseColumnsCatalog = new RandomAccessFile("data//internalTables//davisbase_columns.tbl", "rw");
				davisbaseColumnsCatalog.setLength(pageSize);
				davisbaseColumnsCatalog.seek(0);       
				davisbaseColumnsCatalog.writeByte(0x0D); 
				davisbaseColumnsCatalog.writeByte(0x08); 
				int[] offset=new int[10];
				offset[0]=pageSize-43;
				offset[1]=offset[0]-47;
				offset[2]=offset[1]-44;
				offset[3]=offset[2]-48;
				offset[4]=offset[3]-49;
				offset[5]=offset[4]-47;
				offset[6]=offset[5]-57;
				offset[7]=offset[6]-49;
				offset[8]=offset[7]-49;
				davisbaseColumnsCatalog.writeShort(offset[8]); 
				davisbaseColumnsCatalog.writeInt(0); 
				davisbaseColumnsCatalog.writeInt(0); 
				
				for(int i=0;i<9;i++)
					davisbaseColumnsCatalog.writeShort(offset[i]);

				
				davisbaseColumnsCatalog.seek(offset[0]);
				davisbaseColumnsCatalog.writeShort(33); 
				davisbaseColumnsCatalog.writeInt(1); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(28);
				davisbaseColumnsCatalog.writeByte(17);
				davisbaseColumnsCatalog.writeByte(15);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				
				davisbaseColumnsCatalog.writeBytes("davisbase_tables"); 
				davisbaseColumnsCatalog.writeBytes("rowid"); 
				davisbaseColumnsCatalog.writeBytes("INT"); 
				davisbaseColumnsCatalog.writeByte(1); 
				davisbaseColumnsCatalog.writeBytes("NO"); 
				
				
				davisbaseColumnsCatalog.seek(offset[1]);
				davisbaseColumnsCatalog.writeShort(39); 
				davisbaseColumnsCatalog.writeInt(2); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(28);
				davisbaseColumnsCatalog.writeByte(22);
				davisbaseColumnsCatalog.writeByte(16);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				
				davisbaseColumnsCatalog.writeBytes("davisbase_tables"); 
				davisbaseColumnsCatalog.writeBytes("table_name");  
				davisbaseColumnsCatalog.writeBytes("CHAR"); 
				davisbaseColumnsCatalog.writeByte(2); 
				davisbaseColumnsCatalog.writeBytes("NO"); 
				
				
				davisbaseColumnsCatalog.seek(offset[2]);
				davisbaseColumnsCatalog.writeShort(34); 
				davisbaseColumnsCatalog.writeInt(3); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(29);
				davisbaseColumnsCatalog.writeByte(17);
				davisbaseColumnsCatalog.writeByte(15);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				
				davisbaseColumnsCatalog.writeBytes("davisbase_columns");
				davisbaseColumnsCatalog.writeBytes("rowid");
				davisbaseColumnsCatalog.writeBytes("INT");
				davisbaseColumnsCatalog.writeByte(1);
				davisbaseColumnsCatalog.writeBytes("NO");
				
				
				davisbaseColumnsCatalog.seek(offset[3]);
				davisbaseColumnsCatalog.writeShort(40); 
				davisbaseColumnsCatalog.writeInt(4); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(29);
				davisbaseColumnsCatalog.writeByte(22);
				davisbaseColumnsCatalog.writeByte(16);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				
				davisbaseColumnsCatalog.writeBytes("davisbase_columns");
				davisbaseColumnsCatalog.writeBytes("table_name");
				davisbaseColumnsCatalog.writeBytes("CHAR");
				davisbaseColumnsCatalog.writeByte(2);
				davisbaseColumnsCatalog.writeBytes("NO");
				
				
				davisbaseColumnsCatalog.seek(offset[4]);
				davisbaseColumnsCatalog.writeShort(41); 
				davisbaseColumnsCatalog.writeInt(5); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(29);
				davisbaseColumnsCatalog.writeByte(23);
				davisbaseColumnsCatalog.writeByte(16);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				
				davisbaseColumnsCatalog.writeBytes("davisbase_columns");
				davisbaseColumnsCatalog.writeBytes("column_name");
				davisbaseColumnsCatalog.writeBytes("CHAR");
				davisbaseColumnsCatalog.writeByte(3);
				davisbaseColumnsCatalog.writeBytes("NO");
				
				
				davisbaseColumnsCatalog.seek(offset[5]);
				davisbaseColumnsCatalog.writeShort(39); 
				davisbaseColumnsCatalog.writeInt(6); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(29);
				davisbaseColumnsCatalog.writeByte(21);
				davisbaseColumnsCatalog.writeByte(16);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				davisbaseColumnsCatalog.writeBytes("davisbase_columns");
				davisbaseColumnsCatalog.writeBytes("data_type");
				davisbaseColumnsCatalog.writeBytes("CHAR");
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeBytes("NO");
				
				
				davisbaseColumnsCatalog.seek(offset[6]);
				davisbaseColumnsCatalog.writeShort(49); 
				davisbaseColumnsCatalog.writeInt(7); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(29);
				davisbaseColumnsCatalog.writeByte(28);
				davisbaseColumnsCatalog.writeByte(19);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				
				davisbaseColumnsCatalog.writeBytes("davisbase_columns");
				davisbaseColumnsCatalog.writeBytes("ordinal_position");
				davisbaseColumnsCatalog.writeBytes("TINYINT");
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeBytes("NO");
								
				davisbaseColumnsCatalog.seek(offset[7]);
				davisbaseColumnsCatalog.writeShort(41); 
				davisbaseColumnsCatalog.writeInt(8); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(29);
				davisbaseColumnsCatalog.writeByte(23);
				davisbaseColumnsCatalog.writeByte(16);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				davisbaseColumnsCatalog.writeBytes("davisbase_columns");
				davisbaseColumnsCatalog.writeBytes("is_nullable");
				davisbaseColumnsCatalog.writeBytes("CHAR");
				davisbaseColumnsCatalog.writeByte(6);
				davisbaseColumnsCatalog.writeBytes("NO");
			}
			catch (Exception e) 
			{
				System.out.println("Unable to create the database_columns file");
				System.out.println(e);
			}
	}
	
}
