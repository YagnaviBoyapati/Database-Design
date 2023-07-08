// Created date : 11/23/2022
//  Description : Droping the table
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////




import java.io.RandomAccessFile;
import java.nio.file.*;
import java.io.IOException;

public class DavisBaseDropTable 
{
    public static void drop_Table(String file)
    {
        //1 check to make sure database is valid state
        Path directory = Paths.get("data");
		Path tables = Paths.get("data/davisbase_tables.tbl");
		Path columns = Paths.get("data/davisbase_columns.tbl");
		Path ToBeDeleted = Paths.get("data/"+ file+".tbl");

		if( !Files.exists(ToBeDeleted)) {
			System.out.println("Table " + file +" does not exist in the database. Nothing to drop!");
		}
		else {
			

			if(!Files.isDirectory(directory) || !Files.exists(tables) || !Files.exists(columns))
			{
				System.out.println("db is not initilized properly cannot drop table");
	            return;
			}
	        if(file.equals("davisbase_tables") || file.equals("davisbase_columns"))
	        {
	            System.out.println("Cannot remove " + file + "as it is required for database");
	            return;
	        }
	        //Attempt to delete file
	        try
	        {
	            Files.deleteIfExists(Paths.get("data/" + file + ".tbl"));
	        }
	        catch(NoSuchFileException e)
	        {
	            System.out.println("Table is not in database therefore cannot delete it");
	            return;
	        }
	        catch(DirectoryNotEmptyException e)
	        {
	            System.out.println("Directory is not empty.");
	        }
	        catch(IOException e)
	        {
	            System.out.println("Invalid permissions.");
	        }

	        //Need to delete record from table_file
	        try
	        {
	            RandomAccessFile davisT = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
	            //handling linked list style file
	            boolean flag = false;
	            int n_pages = (int)davisT.length()/512;
	            short spot;
	            byte [] table_name;
	            for(int i = 0; i<n_pages; i++ )
	            {
	                //read number of records from a page
	                davisT.seek(i*512 + 2);
	                short n_records = davisT.readShort();
	                //get ready to read first record
	                int record = 0x10 + i*512;
	                for(short j = 0; j<n_records ; j++)
	                {
	                    davisT.seek(record);
	                    spot = davisT.readShort();
	                    //record header is 6 bytes numcolumn is 1 byte column len is 2 bytes 
	                    //reading len of the string
	                    int len;
	                    davisT.seek(spot+7 + i*512);
	                    len = (davisT.readByte()  & 0xFF) -12; 
	                    table_name = new byte[len];
	                    davisT.read(table_name,0,len);
	                    String tb_Name = new String(table_name);
	                    //if the value == file then remove pointer
	                    //reduce value of records on page
	                    //check to see if it in the pointer most recent record if it is fix it
	                    if(tb_Name.equals(file))
	                    {
	                        
	                        //pointer removed
	                        flag = true;
	                        davisT.seek(record);
	                        davisT.writeShort(0);
	                        //need to move pointers down
	                        decrement_records(j,n_records,i,davisT);
	                        //decrement record count 
	                        davisT.seek(i*512+0x02);
	                        n_records = (short) (n_records -1);
	                        davisT.writeShort(n_records);
	                        
	                        //realign most recent pointer
	                        change_recent_pointer(i,davisT);
	                        break;
	                    }
	                    if(flag == true){break;}
	                    record = record + 2;
	                }
	            }
	            davisT.close();
	            
	        }
	        catch(IOException e)
	        {

	        }
	        
	        //delete records from davisbase_columns
	        try
	        {
	            RandomAccessFile davisC = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
	            int n_pages = (int)davisC.length()/512;
	            byte [] table_name;
	            short spot;
	            for(int x = 0; x < n_pages; x++)
	            {
	                davisC.seek(x*512 + 2);
	                short n_records = davisC.readShort();

	                int record = 0x10 + x*512;
	                for(short j = 0; j<n_records; j++)
	                {
	                    davisC.seek(record);
	                    spot = davisC.readShort();
	                    // 6 for page header 1 for numcolumns 
	                    davisC.seek(spot + 7 + x*512);
	                    int len = (davisC.readByte()  & 0xFF) -12; 
	                    table_name = new byte[len];
	                    davisC.seek(spot + 12 +x*512);
	                    davisC.read(table_name,0,len);
	                    String tb_Name = new String(table_name);
	                    //if the value == file then remove pointer
	                    //reduce value of records on page
	                    //check to see if it in the pointer most recent record if it is fix it
	                    if(tb_Name.equals(file))
	                    {
	                        
	                        //pointer removed
	                        davisC.seek(record);
	                        davisC.writeShort(0);
	                        decrement_records(j, n_records, x, davisC);
	                        //decrement record count 
	                        davisC.seek(0x02 +x*512);
	                        n_records = (short) (n_records -1);
	                        davisC.writeShort(n_records);

	                        //check recent record pointer
	                        change_recent_pointer(x, davisC);
	                        record = record - 2;
	                        j--;
	                        
	                    }

	                    record = record + 2;
	                }
	               
	            }
	            System.out.println(file+ " Table Dropped!!");
	            davisC.close();
	            
	        }
	        catch(IOException e)
	        {

	        }
	        
		}
        
    }
    public static void change_recent_pointer(int page, RandomAccessFile file)
    {
        try
        {
            file.seek(page*DavisBasePrompt.pageSize + 2);
            short num_recs = file.readShort();
            short pointer;
            file.seek(page*DavisBasePrompt.pageSize + 16 + (num_recs-1)*2);
            pointer = file.readShort();
            file.seek(page*DavisBasePrompt.pageSize + 4);
            file.writeShort(pointer);
        }
        catch(IOException e)
        {

        }
    }
    public static void decrement_records(int deleted, int total, int page, RandomAccessFile file)
    {
        try
        {
            for(int i = deleted ;i<total; i++)
            {
                file.seek(page*DavisBasePrompt.pageSize + 16 + (i+1)*2);
                short next = file.readShort();
                file.seek(page*DavisBasePrompt.pageSize + 16 + i*2);
                file.writeShort(next);
            }
        }
        catch(IOException e)
        {

        }
    }

    public static int get_row_id(RandomAccessFile table ,int n_pages) throws IOException
    {
        
        if(n_pages == 0)
        {
            table.seek(4);
            short last_rec = table.readShort();
            if(last_rec == 0)
            {
                return 0;
            }
            else
            {
                table.seek(last_rec + 2);
                return table.readInt() ;
            }

        }
        else
        {
            table.seek(4 + n_pages*DavisBasePrompt.pageSize);
            short last_rec = table.readShort();
            if(last_rec == 0)
            {
                return get_row_id(table,n_pages-1);
            }
            else
            {
                table.seek(last_rec + 2);
                return table.readInt() ;
            }
        }
        
    }
}
