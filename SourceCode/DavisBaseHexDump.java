
// Created date : 11/23/2022
// Description : Hex dump of the db system
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.io.IOException;
import static java.lang.System.out;
import java.util.Arrays;

public class DavisBaseHexDump 
{
	

	static String col_gap = " ";
	static int pageSize = 0x200;
	static RandomAccessFile raf;
	static String con_char= ".";
	static int curr_col = 0;

	static boolean ASCII = true;

	static boolean header_page = true;
	static boolean help = false;


	static void displayHexDump(RandomAccessFile raf)
	 {

		try {

			raf.seek(0); 
			

			int offset = 0;

			long size = raf.length(); 

			byte[] byte_rows = new byte[16];

	
			while(offset < size) {


				if(offset % pageSize == 0) {
					printingpage();
				}

				if(offset % 16 == 0) {

					out.print(String.format("%08x  ", offset));
					curr_col = 0;
				}

				{
					int ndx = offset % 16;
					byte_rows[ndx] = raf.readByte();
					offset++;
					curr_col++;
				}

				if(offset % 16 == 0) {
					printingbytes(byte_rows);

					byte_rows = new byte[16];
				}
				curr_col++;
			}

			printingbytes(byte_rows);
			out.println("Printing Byte Column: " + curr_col);

			out.println();
		} 
		catch (IOException e) {
			out.println(e);
		} 
	}

	static void printingpage() {

		out.println();

		out.print("Address    0  1  2  3  4  5  6  7 " + col_gap + " 8  9  A  B  C  D  E  F");

		if(ASCII)
			out.print("  |0123456789ABCDEF|");
		out.println();


		out.print(line(58,"-"));
		if(ASCII)
			out.print(line(20,"-"));
		out.println();


	}

	static void printingbytes(byte[] row) {
		int rowLength = row.length;

		for(int n = 0; n < rowLength; n++) {
			if(n==8)
				out.print(col_gap);
			out.print(String.format("%02X ", row[n]));
		}
		
		if(ASCII) {

			out.print(" |");

			for(int n = 0; n < rowLength; n++) {
				if(row[n] < 0x20 || row[n] > 0x7e)
					out.print(con_char);
				else
					out.print((char)row[n]);
			}
			out.print("|");
		}
		
		out.println();
	}

	static void copyright_display() {
		out.println("*");
		out.println("* HexDump Chris Irwin Davis");
		out.println("*");
	}


	static String line(int len, String x)
	 {
		String c = "";
		while(len>0) {
			c += x;
			len--;
		}
		return c;
	}
	
	public static void parsingdump(String com)
	 {
		try {
			ArrayList<String> tokens = new ArrayList<>(Arrays.asList(com.split(" ")));

			if (tokens.size() != 2) {
				System.out.println("Error: Enter a valid Command");
			}
			String table_name = tokens.get(1);
			String  file_name = "./data/" + table_name + ".tbl";
			RandomAccessFile table_file = new RandomAccessFile(file_name, "r");
			displayHexDump(table_file);
			table_file.close();
		} catch (Exception e) {
			System.out.println("Error: Failed with the execution");
			System.out.println(e);
		}
	}
}
