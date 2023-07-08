
// Created date : 11/23/2022
// Description : select statements
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


import static java.lang.System.out;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;

public class DavisBaseSelect {
	
	public static void commandUsage() {
		System.out.println("Invalid command");
		System.out.println("USAGE:     SELECT * FROM <table_name>;");
		System.out.println("USAGE:     SELECT * FROM <table_name> WHERE row_id=<row_id>;");
		System.out.println("USAGE:     SELECT * FROM <table_name> WHERE NOT row_id=<row_id>;");
		System.out.println("USAGE:     SELECT <column_name> FROM <table_name> WHERE row_id=<row_id>;");
		System.out.println("USAGE:     SELECT <column_name> FROM <table_name> WHERE NOT row_id=<row_id>;");
	}
	
	public static void parse_query(String cmd_string) {
		ArrayList<String> cmd_tokens = new ArrayList<String>(Arrays.asList(cmd_string.split(" ")));
		if (cmd_tokens.size() < 4 || cmd_tokens.size() > 7) {
			commandUsage();
			return;
		}
		//if (cmd_tokens.get(1).equals("*")) {
		if (cmd_tokens.get(2).equals("from")) {
			String table_name = cmd_tokens.get(3).trim();
			String table_file_name = table_name + ".tbl";
			if (!DavisBaseUtils.isTablePresent(table_file_name)) {
				System.out.println("Table name does not exists");
			} else {
				if (cmd_tokens.size() > 4) {
					if (cmd_tokens.get(4).equals("where")) {
						boolean negate = false;
						String row_id_string = "";
						if (cmd_tokens.size() == 7) {
							if (cmd_tokens.get(5).equals("not")) {
								negate = true;
								row_id_string = cmd_tokens.get(6);
							} else {
								commandUsage();
							}
						} else {
							row_id_string = cmd_tokens.get(5);
						}
						
						if (row_id_string.contains(">=")) {
							if ((row_id_string.substring(0, row_id_string.indexOf(">="))).equals("row_id")) {
								int row_id = Integer.parseInt(row_id_string.substring(row_id_string.indexOf(">=")+2));
								run_query(table_name, row_id, ">=", negate, cmd_tokens.get(1));
							} else {
								System.out.println("command not supported");
								return;
							}
						} else if (row_id_string.contains("<=")) {
							if ((row_id_string.substring(0, row_id_string.indexOf("<="))).equals("row_id")) {
								int row_id = Integer.parseInt(row_id_string.substring(row_id_string.indexOf("<=")+2));
								run_query(table_name, row_id, "<=", negate, cmd_tokens.get(1));
							} else {
								System.out.println("command not supported");
								return;
							}
						} else if (row_id_string.contains("<>")) {
							if ((row_id_string.substring(0, row_id_string.indexOf("<>"))).equals("row_id")) {
								int row_id = Integer.parseInt(row_id_string.substring(row_id_string.indexOf("<>")+2));
								run_query(table_name, row_id, "<>", negate, cmd_tokens.get(1));
							} else {
								System.out.println("command not supported");
								return;
							}
						} else if (row_id_string.contains(">")) {
							if ((row_id_string.substring(0, row_id_string.indexOf(">"))).equals("row_id")) {
								int row_id = Integer.parseInt(row_id_string.substring(row_id_string.indexOf(">")+1));
								run_query(table_name, row_id, ">", negate, cmd_tokens.get(1));
							} else {
								System.out.println("command not supported");
								return;
							}
						} else if (row_id_string.contains("<")) {
							if ((row_id_string.substring(0, row_id_string.indexOf("<"))).equals("row_id")) {
								int row_id = Integer.parseInt(row_id_string.substring(row_id_string.indexOf("<")+1));
								run_query(table_name, row_id, "<", negate, cmd_tokens.get(1));
							} else {
								System.out.println("command not supported");
								return;
							}
						} else if (row_id_string.contains("=")) {
							if ((row_id_string.substring(0, row_id_string.indexOf("="))).equals("row_id")) {
								int row_id = Integer.parseInt(row_id_string.substring(row_id_string.indexOf("=")+1));
								run_query(table_name, row_id, "=", negate, cmd_tokens.get(1));
							} else {
								System.out.println("command not supported");
								return;
							}
						} else {
							commandUsage();
						}
					} else {
						commandUsage();
					}
				} else {
					if (!cmd_tokens.get(1).equals("*")) {
						commandUsage();
					} else {
						run_query(table_name, 0, "", false, cmd_tokens.get(1));		// print all records
					}
				}
			}
		} else {
			commandUsage();
		}
	}
	
	public static void run_query(String table_Name, int row_id, String oper, boolean negate, String columnName) {
		boolean flag = false;
		try {
			RandomAccessFile query_table = new RandomAccessFile("data/" + table_Name + ".tbl", "rw");
			ArrayList<Integer> lsst = new ArrayList<>();
			DavisBaseUtils.extractCellOffsets(lsst, query_table);
			//System.out.println("Size of lsst: " + lsst.size());
			HashMap<String, String> column_to_type = new HashMap<String, String>();
			HashMap<Integer, String> column_to_ord = new HashMap<Integer, String>();
			HashMap<String, String> column_to_null = new HashMap<String, String>();
			DavisBaseUtils.extractColmnNames(table_Name, column_to_ord, column_to_type, column_to_null);
			if (columnName.equals("*")) {
				DavisBaseUtils.printTableHeader(column_to_ord);
			}
					
			for (int i=0; i<lsst.size(); i++) {
				query_table.seek(lsst.get(i)+2);		// Set file pointer within the file so the starting byte of cell/record
				int id = query_table.readInt();
				if (oper.equals("")) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
				} else if (oper.equals("=") && !negate && (id == row_id)) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
					break;
				} else if (oper.equals("=") && negate && (id != row_id)) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
				} else if (oper.equals(">") && !negate && (id > row_id)) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
				} else if (oper.equals(">") && negate && (id <= row_id)) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
				} else if (oper.equals("<") && !negate && (id < row_id)) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
				} else if (oper.equals("<") && negate && (id >= row_id)) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
				} else if (oper.equals(">=") && !negate && (id >= row_id)) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
				} else if (oper.equals(">=") && negate && (id < row_id)) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
				} else if (oper.equals("<=") && !negate && (id <= row_id)) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
				} else if (oper.equals("<=") && negate && (id > row_id)) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
				} else if (oper.equals("<>") && !negate && (id != row_id)) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
				} else if (oper.equals("<>") && negate && (id == row_id)) {
					if (!extract_values(column_to_ord, column_to_type, query_table, lsst.get(i), columnName)) {
						System.out.println("Invalid column name");
						flag = true;
						break;
					}
					flag = true;
				}
			}
			if (!flag) {
				System.out.println("Item not flag");
			}
			query_table.close();
			System.out.println();
		} catch (Exception e) {
			out.println("Unable to get the values from the DB tables");
			out.println(e);
		}
	}

	public static boolean extract_values(HashMap<Integer, String> column_to_ord,
									 HashMap<String, String> column_to_type, 
									 RandomAccessFile ref, Integer offSet, String columnName) {
		boolean flag = false;
		try {
			int item_Offset = offSet + 7;
			int offset_size = offSet + 7;
			Formatter f = new Formatter();			// create a Formatter to format the output in proper alignment
			
			ref.seek(offSet+2);
			int row_id = ref.readInt();
			f.format("%d", row_id);
			int n_cols = ref.readByte();
			item_Offset = item_Offset + n_cols;
			
			for(int i = 0; i < n_cols; i++) {
				String colName = column_to_ord.get(i);
				String colType = column_to_type.get(colName);
				if (!columnName.equals("*") && !columnName.equals(colName)) {
					ref.seek(offset_size);
					int code = (int) ref.readByte();
					offset_size = offset_size + 1;
					int elemSize = DataType.getSize(code);
					item_Offset = item_Offset + elemSize;
					continue;
				}
				flag = true;
				ref.seek(offset_size);
				int code = (int) ref.readByte();
				offset_size = offset_size + 1;
				int elemSize = DataType.getSize(code);
				if (elemSize == 0) {
					f.format("%18s", "Null");
				} else {
					byte[] b = new byte[elemSize];
					ref.seek(item_Offset);
					item_Offset = item_Offset + elemSize;
					ref.readFully(b);

					if (colType.equals("tinyint")) {
						f.format("%18d", (int) b[0]);
					} else if (colType.equals("smallint")) {
						f.format("%18d", ByteBuffer.wrap(b).getShort());
					} else if (colType.equals("int")) {
						f.format("%18d", ByteBuffer.wrap(b).getInt());
					} else if (colType.equals("bigint")) {
						f.format("%18d", ByteBuffer.wrap(b).getLong());
					} else if (colType.equals("long")) {
						f.format("%18d", ByteBuffer.wrap(b).getLong());
					} else if (colType.equals("float")) {
						f.format("%18f", ByteBuffer.wrap(b).getFloat());
					} else if (colType.equals("double")) {
						f.format("%18f", ByteBuffer.wrap(b).getDouble());
					} else if (colType.equals("year")) {
						f.format("%18d", ByteBuffer.wrap(b).getShort());
					} else if (colType.equals("time")) {
						f.format("%18s", DavisBaseParser.unparseTime(b));
					} else if (colType.equals("datetime")) {
						f.format("%18s", DavisBaseParser.unparseDateTime(b));
					} else if (colType.equals("date")) {
						f.format("%18s", DavisBaseParser.unparseDate(b));
					} else if (colType.equals("text")) {
						String strVal = new String(b);
						f.format("%18s", strVal);
					} else {
						System.out.println("Type not recognized, printing in bytes");
						f.format("%18s", b);
					}
				}
			}
			if (flag) {
				System.out.println(f);
			}
		} catch (Exception e) {
			out.println("Unable to extract values from database_table files");
			out.println(e);
		}
		return flag;
	}
}
