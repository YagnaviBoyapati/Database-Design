/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Created date : 11/23/2022
// Description : Creating tables for Db
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.nio.ByteBuffer;

import static java.lang.System.out;

public class DavisBaseCreateTable {

	/**
	 *  Stub method for creating new tables
	 *  @param queryString is a String of the user input
	 *  
	 *  Sample create command,
	 *  
	 *  CREATE TABLE CONTACT (
  	 * 		Contact_id	INT NOT NULL,
  	 *		Fname	 	TEXT NOT NULL,
  	 *		Mname		TEXT,
  	 *		Lname		TEXT NOT NULL
	 *	);
	 */
	public static void parse_CreateTable(String cndString) {
		boolean isTypeValid = true;
		ArrayList<String> cndToken = new ArrayList<String>(Arrays.asList(cndString.split("\s+")));
		
		if (cndToken.get(1).equals("table")) {
			//System.out.println("Parsing the string:\"" + cndString + "\"");
			String column = cndString.substring(cndString.indexOf('(')+1,  cndString.lastIndexOf(')')).trim();
			ArrayList<String> TokensColmn = new ArrayList<String>(Arrays.asList(column.split(",")));
			DavisBaseUtils.trimArrayFields(TokensColmn);
			
			for(int i=0; i < TokensColmn.size() ; i++) {
				ArrayList<String> DetailsColmn = 
						new ArrayList<String>(Arrays.asList(TokensColmn.get(i).replaceAll("[\t]+", " ").replace("\n", " ").replaceAll("[ ]+", " ").trim().split(" ")));
				if(!isValidDatatype(DetailsColmn.get(1).toLowerCase())) {
					isTypeValid = false;
				}
				
			}
			
			if(isTypeValid) {
				/* Define table file name */
				String TableName = cndToken.get(2).trim();
				String TableFileName = TableName + ".tbl";
			
				if (DavisBaseUtils.isTablePresent(TableFileName)) {
					System.out.println("ERROR!---Given Table name already exists");
				} else {
					TableCreate(TableFileName);
					/*  Code to insert a row in the davisbase_tables table 
					 *  i.e. database catalog meta-data 
					 */
					TableAdd(TableName);
					
					/*  Code to insert rows in the davisbase_columns table  
					 *  for each column in the new table 
					 * 	i.e. database catalog meta-data  colmTokens.size()
				 	*/
					for(int i=0; i < TokensColmn.size() ; i++) {
						String DetailsOfColumn = TokensColmn.get(i).replaceAll("[\t]+", " ").replace("\n", " ").replaceAll("[ ]+", " ").trim();
						addColmnName(DetailsOfColumn, TableName, i);
					}
					
					System.out.println(TableName + " Table sucessfully created!!");
				}
			} else {
				System.out.println("Table cannot be created! Invalid Dataype(s) id given");
			}
		} else if (cndToken.get(1).equals("index")) {
			
		} else {
			System.out.println("Given Command is invalid!");
		}
	}
	
	public static void TableCreate(String TableName) {
		/*  Code to create a .tbl file to contain table data */
		try {
			RandomAccessFile CreateFile = new RandomAccessFile("data/" + TableName, "rw");
			CreateFile.setLength(DavisBaseUtils.pageSize);
			CreateFile.seek(0);
			CreateFile.writeByte(0x0D);
			CreateFile.seek(DavisBaseUtils.leafOrNode);
			CreateFile.writeShort(65535);
			CreateFile.writeShort(65535);
			CreateFile.writeShort(65535);
			CreateFile.writeShort(65535);
			CreateFile.close();
			
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}
	
	/**
	 * Method to write the table name into the davisbase_tables.tbl file.
	 * This is the first function called from DavisBasePrompt.java when 
	 * new table is created by user.
	 * @param TableName
	 */
	public static void TableAdd(String TableName) {
		try {
			RandomAccessFile CatalogTable = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			/**
			 * construct the cell with 
			 * 2 bytes of payLoad size + 4 bytes of rowId + 1 bytes for number of columns + 
			 * 1 bytes for each column size.. + payLoad
			 */
			ByteBuffer PayLoadSize = ByteBuffer.allocate(2); 		// 2 bytes for payload size
			PayLoadSize.putShort((short) TableName.length());		// add size of payload (table name length here)
			
			ByteBuffer ByteRowId = ByteBuffer.allocate(4);			// 4 bytes for row id
			int currentRowId = DavisBaseUtils.getCurrentRowId(CatalogTable); 
			ByteRowId.putInt((int) ++currentRowId);
			
			byte[] recordIndex = new byte[2];						// 1 bytes for number of columns and 1 for its size
			Array.setByte(recordIndex, 0, (byte) 1);				// There is only one column in davisbase_tables
			Array.setByte(recordIndex, 1, (byte) (12 + TableName.length()));	// size of payload is size of table name + 12 (to indicate that the payload is of type TEXT)

			byte [] payLoad = TableName.getBytes();
			
			int CellSize = PayLoadSize.array().length + ByteRowId.array().length + recordIndex.length + payLoad.length;  // total size of the cell

			List<Byte> ary = new ArrayList<Byte>();				// ArrayList of bytes to append all the above
			byte[] sz = PayLoadSize.array();						// returns array of bytes
			byte[] rowid = ByteRowId.array();
			
			DavisBaseUtils.appendBytesToList(ary, sz);			// Append bytes to list
			DavisBaseUtils.appendBytesToList(ary, rowid);
			DavisBaseUtils.appendBytesToList(ary, recordIndex);
			DavisBaseUtils.appendBytesToList(ary, payLoad);
			
			byte[] cellByte = new byte[ary.size()];				// construct array of bytes
			DavisBaseUtils.convertListToByteArray(cellByte, ary);	// copy bytes from list to the array of bytes
			/* Finished cell construction */

			DavisBaseUtils.insertCellBytesIntoPage(CatalogTable, cellByte, CellSize);
			CatalogTable.close();									// Mandatory need for the close of file
		}
		catch (Exception e) {
			out.println("Table name cannot be added into the  davisbase_tables.tbl file");
			out.println(e);
		}
	}
	
	
	/**
	 * Method to write the table name and column names into the davisbase_columns.tbl file.
	 * This is the second function called after writeTableName from DavisBasePrompt.java when 
	 * new table is created by user.
	 * @param arr
	 * @param TableName
	 * @param pos
	 */
	public static void addColmnName(String DetailsOfColumn, String TableName, Integer position) {
		boolean isNull = true;
		if (DetailsOfColumn.contains("not null")) {
			isNull = false;
		}
		ArrayList<String> columnDetailsList = new ArrayList<String>(Arrays.asList(DetailsOfColumn.split(" ")));
		try {
			RandomAccessFile ColumnsCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			/**
			 * construct the cell with payLoad size + rowId + payLoad
			 */
			ByteBuffer PayLoadSize = ByteBuffer.allocate(2);		// 2 bytes for payload size
			
			ByteBuffer ByteRowId = ByteBuffer.allocate(4);			// 4 bytes for row id
			int currentRowId = DavisBaseUtils.getCurrentRowId(ColumnsCatalog);
			ByteRowId.putInt((int) ++currentRowId);
			
			byte[] recordIndex = new byte[6];						// one byte to store number of columns and rest to store size of each column value
			/*
			 * one byte value representing the number of columns. 
			 * Number of columns here are 5 in davisbase_columns.tbl  (table name, column name, data_type, ordinal pos, is_nullable)
			 */
			Array.setByte(recordIndex, 0, (byte) 5);
			Array.setByte(recordIndex, 1, (byte) (12 + TableName.length()));				// one byte value representing table name length in the record body
			Array.setByte(recordIndex, 2, (byte) (12 + columnDetailsList.get(0).length()));	// one byte value representing column name length in the record body
			Array.setByte(recordIndex, 3, (byte) (12 + columnDetailsList.get(1).length())); // one byte value representing data type length of column in the record body
			Array.setByte(recordIndex, 4, (byte) 1);										// one byte value representing the ordinal position of column
			if (isNull) {
				Array.setByte(recordIndex, 5, (byte) (12 + 3)); 	// one byte value representing the size of fifth element in record body (is nullable) YES
			} else {
				Array.setByte(recordIndex, 5, (byte) (12 + 2)); 	// one byte value representing the size of fifth element in record body (is nullable) NO
			}
			
			List<Byte> payLoad = new ArrayList<Byte>();
			byte[] byte1 = TableName.getBytes();						
			DavisBaseUtils.appendBytesToList(payLoad, byte1);		// table name goes into payload as first entry
			byte[] byte2 = columnDetailsList.get(0).getBytes();		
			DavisBaseUtils.appendBytesToList(payLoad, byte2);		// column name goes into payload as second entry
			byte[] byte3 = columnDetailsList.get(1).getBytes();
			DavisBaseUtils.appendBytesToList(payLoad, byte3);		// column data type goes into payload as third entry
			byte byte4 = position.byteValue();
			payLoad.add(byte4);									// ordinal position as forth entry
			String nullAble = "NO";
			if (isNull) {
				nullAble = "YES";
			}
			byte[] byte5 = nullAble.getBytes();					// "YES" or "NO" goes into payload as fifth entry
			DavisBaseUtils.appendBytesToList(payLoad, byte5);  

			PayLoadSize.putShort((short) payLoad.size());			// put the complete size of payload in to PayLoadSize (first two bytes of cell)
			
			// Calculate the total cell size
			int CellSize = PayLoadSize.array().length + ByteRowId.array().length
						   + recordIndex.length + payLoad.size();
			
			List<Byte> cellArray = new ArrayList<Byte>();
			byte[] sz = PayLoadSize.array();
			byte[] rowid = ByteRowId.array();
			
			DavisBaseUtils.appendBytesToList(cellArray, sz);
			DavisBaseUtils.appendBytesToList(cellArray, rowid);
			DavisBaseUtils.appendBytesToList(cellArray, recordIndex);
			cellArray.addAll(payLoad);
			
			byte[] cellByte = new byte[cellArray.size()];
			DavisBaseUtils.convertListToByteArray(cellByte, cellArray);
			/* Finished cell construction */
	
			DavisBaseUtils.insertCellBytesIntoPage(ColumnsCatalog, cellByte, CellSize);
			ColumnsCatalog.close();
		} catch (Exception e) {
			out.println("Column names cannot be added into the in davisbase_columns.tbl file");
			out.println(e);
		}
	}
	
	public static int getPageSize() {
		return DavisBaseUtils.pageSize;
	}
	
	public static boolean isValidDatatype(String check) {
        switch (check) {
            case "tinyint":
            case "smallint":
            case "int":
            case "bigint":
            case "long":
            case "float":
            case "double":
            case "year":
            case "time":
            case "datetime":
            case "date":
            case "text":
                return true;
            default:
                return false; 
        }
    }
}
