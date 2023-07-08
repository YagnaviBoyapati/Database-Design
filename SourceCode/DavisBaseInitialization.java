
// Created date : 11/23/2022
// Description : Utils class for the DB system
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


import static java.lang.System.out;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DavisBaseInitialization {


	static int page_power= 9;
	
	static int page_size = (int)Math.pow(2, page_power);
	static int leaf	= 6;


	public static void initializeDataStore() 
	{
		Path dir = Paths.get("data");
        Path table = Paths.get("data/davisbase_tables.tbl");
        Path columns = Paths.get("data/davisbase_columns.tbl");
        if(!Files.isDirectory(dir) || !Files.exists(table) || !Files.exists(columns))
        {
            System.out.println("Database does not exist !!");
        	System.out.println("Initializing  database....");
        	try {
        		File datadir = new File("data");
        		datadir.mkdir();
        		String[] files_old;
        		files_old = datadir.list();
        		for (int i=0; i<files_old.length; i++) {
        			File old_file = new File(datadir, files_old[i]);

        			if (old_file.delete()) {

        			} else {

        			}
        		}
        	} catch (SecurityException s) {
        		out.println("Unable to create data container directory");
        		out.println(s);
        	}
        	

        	try {
        		RandomAccessFile tables_catalog = new RandomAccessFile("data/davisbase_tables.tbl", "rw");

        		tables_catalog.setLength(page_size);

        		tables_catalog.seek(0);

        		tables_catalog.writeByte(0x0D);

        		tables_catalog.seek(leaf);
 
        		tables_catalog.writeShort(65535);
        		tables_catalog.writeShort(65535);
   
        		tables_catalog.writeShort(65535);
        		tables_catalog.writeShort(65535);

        		tables_catalog.close();
        	} catch (Exception e) {
        		out.println("Couldn't create the file");
        		out.println(e);
        	}

        	try {
        		RandomAccessFile col_catalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
  
        		col_catalog.setLength(page_size);
        		col_catalog.seek(0);      

        		col_catalog.writeByte(0x0D);

        		col_catalog.seek(leaf);
        		col_catalog.writeShort(65535);
        		col_catalog.writeShort(65535);
        		col_catalog.writeShort(65535);
        		col_catalog.writeShort(65535);
        		col_catalog.close();
        		System.out.println("Database is successfully Initialized!!");
        	} catch (Exception e) {
        		out.println("Couldn't create file");
        		out.println(e);
        	}
        }
	}   
}
