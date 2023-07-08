
// Created date : 11/22/2022
// Description :  Updating record in DB system
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.*;


public class DavisBaseUpdateRecord
{
    public static void setupPage(int Page ,RandomAccessFile raf) throws IOException {
        raf.seek(Page*DavisBasePrompt.pageSize);
        raf.writeByte(0x0D);
        raf.seek(2);
        raf.writeShort(0);
        raf.writeShort(0);
        raf.writeInt(-1);
        raf.writeInt(-1);
    }
    
    public static void updateRecord(String updated_command)
    {
        ArrayList<String> tokens_update = new ArrayList<String>(Arrays.asList(updated_command.split(" ")));
        String table_name = tokens_update.get(1), logic;
        if(!tokens_update.get(0).equals("update") || !tokens_update.get(2).equals("set")){
            System.out.print("\nMissing update or set term of update record command");
            return;
        }
        //make sure where and rowid are present
        if(!updated_command.contains("where") || !updated_command.contains("rowid")) {
            System.out.print("\nMissing where or rowid for update command");
            return;
        }
        //make sure table is present
        Path table_path = Paths.get("data/" + table_name + ".tbl");
        Map<String, Column> columnMap = new HashMap<String, Column>();
        Map<Integer, String> ordinalMap = new HashMap<Integer, String>();
        ArrayList<String> get_tokens_update;
        if(!Files.exists(table_path) ) { System.out.print("\n" + table_name + " table does not exsist in database"); return;}
        if(table_name.equals("davisbase_columns") || table_name.equals("davisbase_tables")) { System.out.print("\nNot allowed to update tables or columns tabbles"); return;}
        if(updated_command.contains("<=")) logic = "<=";
        else if(updated_command.contains(">=")) logic = ">=";
        else if(updated_command.contains("<")) logic = "<";
        else if(updated_command.contains(">")) logic = ">";
        else logic ="=";
        updated_command = updated_command.replaceAll(logic," ");
        updated_command = updated_command.replaceAll("\\s{2,}", " ");
        String field_values = updated_command.substring(updated_command.indexOf("set")+3, updated_command.indexOf("where")-1).trim();
        if(field_values.contains(", ")) get_tokens_update = new ArrayList<String>(Arrays.asList(field_values.split(", ")));
        else get_tokens_update = new ArrayList<String>(Arrays.asList(field_values.split(",")));
        String rowid_text = updated_command.substring(updated_command.indexOf("rowid")+5, updated_command.length()).trim();
        int rowid = Integer.parseInt(rowid_text);
        try {
            RandomAccessFile f_table = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
            int page_no = (int) (f_table.length()/DavisBasePrompt.pageSize);
            for(int i=0; i<page_no; i++) {
                int spots = 16;
                f_table.seek(i*DavisBasePrompt.pageSize + 2);
                short recs_no = f_table.readShort();
                for(int j = 0; j<recs_no; j++) {
                    byte ordinal;
                    String type, rowName, mytable;
                    f_table.seek(i*DavisBasePrompt.pageSize + spots);
                    short recPointer = f_table.readShort();
                    f_table.seek(i*DavisBasePrompt.pageSize + recPointer+7);
                    int lengthOfTable = (f_table.readByte()  & 0xFF) -12;
                    byte [] tableBytes = new byte[lengthOfTable];

                    int lengthOfColumn = (f_table.readByte()  & 0xFF) -12, lengthOfType = (f_table.readByte()  & 0xFF) -12;
                    byte [] columnBytes = new byte[lengthOfColumn], typeBytes = new byte[lengthOfType];

                    f_table.readByte();

                    int lengthOfNull = (f_table.readByte()  & 0xFF) -12;
                    f_table.read(tableBytes,0,lengthOfTable);
                    mytable = new String(tableBytes);
                    if(mytable.equals(table_name)) {
                        f_table.read(columnBytes,0,lengthOfColumn);
                        rowName = new String(columnBytes);
                        f_table.read(typeBytes,0,lengthOfType);
                        type = new String(typeBytes);

                        ordinal = f_table.readByte();
                        if(lengthOfNull > 2) {
                            Column column = new Column(mytable,rowName,type,"yes",(int) ordinal);
                            columnMap.put(rowName,column);
                            ordinalMap.put((int)ordinal,type);
                        } else {
                            Column column = new Column(mytable,rowName,type,"no",(int) ordinal);
                            columnMap.put(rowName,column);
                            ordinalMap.put((int)ordinal,type);
                        }
                    }
                    spots += 2;
                }
            }
            f_table.close();
        }
        catch(IOException e) {System.out.print("");}
        Update[] update_array = new Update[columnMap.size()];
        for(int i = 0; i<get_tokens_update.size();i++) {
            String parseColumn = get_tokens_update.get(i);
            ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(parseColumn.split(" ")));
            Column temp = columnMap.get(columnTokens.get(0));
            if(temp == null) {
                System.out.println("Entered " + columnTokens.get(0) + " which table " + table_name + " doesn't have a column for");
                return;
            } else {
                update_array[temp.ordinal] = new Update(temp.type,columnTokens.get(1),temp.ordinal);
                if(temp.type.equals("text")) update_array[temp.ordinal].length = update_array[temp.ordinal].value.length();
            } 
            
        }
        
        //sort list for ease of use
        for(int i = 0; i<update_array.length;i++) {
            for(int j =i+1; j<update_array.length; j++) {
                if(update_array[i] != null && update_array[j] != null && update_array[i].ordinal > update_array[j].ordinal) {
                    Update temp = update_array[i];
                    update_array[i] = update_array[j];
                    update_array[j] = temp;
                }
            }
        }
        
        int maxRowid = 0;
        if((logic == "<=") || (logic == "<") || (logic == ">") || (logic == ">=")){
            try {
                RandomAccessFile checkFile = new RandomAccessFile("data/" + table_name + ".tbl", "rw");
                maxRowid = DavisBaseDropTable.get_row_id(checkFile, (int) (checkFile.length() / DavisBasePrompt.pageSize - 1));
                checkFile.close();
            } catch (IOException e) {}
        }
        if(logic == "<=") for (int i = 1; i <= rowid && i <= maxRowid; i++) updateRecordById(table_name, update_array, columnMap, ordinalMap, i);
        else if(logic == "<") for (int i = 1; i < rowid && i <= maxRowid; i++) updateRecordById(table_name, update_array, columnMap, ordinalMap, i);
        else if(logic == ">") for (int i = rowid + 1; i <= maxRowid; i++) updateRecordById(table_name, update_array, columnMap, ordinalMap, i);
        else if(logic == ">=") for (int i = rowid; i <= maxRowid; i++) updateRecordById(table_name, update_array, columnMap, ordinalMap, i);
        else updateRecordById(table_name, update_array, columnMap, ordinalMap, rowid);
    }
    
    public static void updateRecordById(String table_name, Update[] update_array,Map<String, Column> columnMap,Map<Integer, String> ordinalMap, int rowid ) {
        Update [] record = new Update [columnMap.size()];
        try {
            RandomAccessFile f_table = new RandomAccessFile("data/" + table_name + ".tbl","rw");
            boolean is_found = false;
            int page_no = (int) (f_table.length()/DavisBasePrompt.pageSize);
            for(int i=0; i<page_no; i++) {
                f_table.seek(i*DavisBasePrompt.pageSize + 2);
                short recs_no = f_table.readShort();
                int spots = 16;
                for(int j = 0; j<recs_no; j++)
                {
                    f_table.seek(i*DavisBasePrompt.pageSize + spots);
                    short recPointer = f_table.readShort();
                    f_table.seek(i*DavisBasePrompt.pageSize + recPointer+2);
                    int recId = f_table.readInt(), z = 0;
                    if(rowid == recId)
                    {
                        is_found = true;
                        int numFields = f_table.readByte() & 0xFF;
                        for(int k = 0; k<numFields;k++)
                        {
                            int length;
                            String updateType = ordinalMap.get(k);
                            if(updateType.equals("text")) length = (f_table.readByte() & 0xFF) - 12;
                            else {
                                f_table.readByte();
                                length = 0;
                            }
                            record[z] = new Update(updateType,length);
                            z++;
                        }
                        
                        for(int k =0; k<numFields; k++)
                        {
                            byte[] readMe;
                            String typ = record[k].type;
                            if(typ=="tinyint") record[k].value = Byte.toString(f_table.readByte());
                            else if (typ == "smallint") record[k].value = Short.toString(f_table.readShort());
                            else if (typ == "int") record[k].value = Integer.toString(f_table.readInt());
                            else if (typ == "bigint") record[k].value = Long.toString(f_table.readLong());
                            else if (typ == "long") record[k].value = Long.toString(f_table.readLong());
                            else if (typ == "float") record[k].value = Float.toString(f_table.readFloat());
                            else if (typ == "double") record[k].value = Double.toString(f_table.readDouble());
                            else if (typ == "year") record[k].value = Byte.toString(f_table.readByte());
                            else if (typ == "time") record[k].value = Integer.toString(f_table.readInt());
                            else if (typ == "datetime") record[k].value = Long.toString(f_table.readLong());
                            else if (typ == "date"){
                                readMe = new byte [8];
                                f_table.read(readMe,0,8);
                                record[k].value = new String(readMe);
                            } else if (typ == "text"){
                                readMe = new byte [record[k].length];
                                f_table.read(readMe,0,record[k].length);
                                record[k].value = new String(readMe);
                            } else {System.out.print("");}
                        }
                        DavisBaseDeleteRecord.deleteRecordById(rowid, f_table);
                        break;
                    }
                    spots += 2;
                }
                if(is_found == true){break;}
            }
            //System.out.println("Could not find record with rowid " + rowid);
            f_table.close();
            if(is_found != true) return;
        }
        catch(IOException e){}
        for(int i = 0; i<record.length; i++) {if(update_array[i]!= null) record[i] = update_array[i];}

        try {
            RandomAccessFile f_table = new RandomAccessFile("data/" + table_name + ".tbl", "rw");
            int page_no = (int) (f_table.length()/DavisBasePrompt.pageSize -1);
            f_table.seek(page_no*DavisBasePrompt.pageSize + 2);
            short recs_no = f_table.readShort(), recPointer = f_table.readShort();
            int payload = 1;
            for(int i = 0; i<record.length; i++) {
                payload++;
                String typ = record[i].type;
                if(typ == "tinyint") payload= payload + 1;
                else if(typ == "smallint") payload +=2;
                else if(typ == "int") payload +=4;
                else if(typ == "bigint") payload+=8;
                else if(typ == "long") payload+=8;
                else if(typ == "float") payload+=4;
                else if(typ == "double") payload+=8;
                else if(typ == "year") payload++;
                else if(typ == "time") payload +=4;
                else if(typ == "datetime") payload+=8;
                else if(typ == "date") payload+=8;
                else if(typ == "text") payload+=record[i].length;
                else System.out.print("");
            }
            int recSize = payload + 6, newRowid = DavisBaseDropTable.get_row_id(f_table,page_no) + 1;
            if(recs_no == 0||recPointer - (16 + (recs_no+1)*2) > (recSize)) {
                if(recs_no == 0) recPointer = (short) DavisBasePrompt.pageSize;
                f_table.seek(page_no*DavisBasePrompt.pageSize + 16 + recs_no*2);
                f_table.writeShort(recPointer - recSize);
                f_table.seek(page_no*DavisBasePrompt.pageSize + 2);
                f_table.writeShort(recs_no + 1);
                f_table.writeShort(recPointer - recSize);
                //write record
                f_table.seek(page_no*DavisBasePrompt.pageSize + recPointer - recSize);
                f_table.writeShort(payload);
                f_table.writeInt(newRowid);
                f_table.writeByte((byte)record.length);
                for(int i =0; i<record.length; i++) {
                    String typ = record[i].type;
                    if(typ == "null") f_table.writeByte(0);
                    else if(typ == "tinyint") f_table.writeByte(1);
                    else if(typ == "smallint") f_table.writeByte(2);
                    else if(typ == "int") f_table.writeByte(3);
                    else if((typ == "bigint") || (typ == "long")) f_table.writeByte(4);
                    else if(typ == "float") f_table.writeByte(5);
                    else if(typ == "double") f_table.writeByte(6);
                    else if(typ == "year") f_table.writeByte(8);
                    else if(typ == "time") f_table.writeByte(9);
                    else if(typ == "datetime") f_table.writeByte(10);
                    else if(typ == "date") f_table.writeByte(11);
                    else if(typ == "text") f_table.writeByte(12 + record[i].length);
                    else System.out.print("");
                }
                for(int i =0; i<record.length; i++) {
                    String typ = record[i].type;
                    if(typ == "tinyint") f_table.writeByte(Byte.parseByte(record[i].value));
                    else if(typ == "smallint") f_table.writeShort(Short.parseShort(record[i].value));
                    else if(typ == "int") f_table.writeInt(Integer.parseInt(record[i].value));
                    else if(typ == "bigint") f_table.writeLong(Long.parseLong(record[i].value));
                    else if(typ == "long") f_table.writeLong(Long.parseLong(record[i].value));
                    else if(typ == "float") f_table.writeFloat(Float.parseFloat(record[i].value));
                    else if(typ == "double") f_table.writeDouble(Double.parseDouble(record[i].value));
                    else if(typ == "year") f_table.writeByte(Byte.parseByte(record[i].value));
                    else if(typ == "time")  f_table.writeInt(Integer.parseInt(record[i].value));
                    else if(typ == "datetime") f_table.writeLong(Long.parseLong(record[i].value));
                    else if(typ == "date") f_table.writeBytes(record[i].value);
                    else if(typ == "text") f_table.writeBytes(record[i].value);
                    else System.out.print("");
                }
            } else {
                f_table.setLength(f_table.length()+ DavisBasePrompt.pageSize);
                page_no++;
                setupPage(page_no, f_table);
                f_table.seek(page_no*DavisBasePrompt.pageSize + 2);
                f_table.writeShort(1);
                f_table.writeShort((short) (DavisBasePrompt.pageSize -recSize));
                f_table.seek(page_no*DavisBasePrompt.pageSize + 16);
                f_table.writeShort((short) (DavisBasePrompt.pageSize -recSize));
                f_table.seek(page_no*DavisBasePrompt.pageSize + DavisBasePrompt.pageSize -recSize);
                f_table.writeShort(payload);
                f_table.writeInt(newRowid);
                f_table.writeByte((byte)record.length);
                for(int i =0; i<record.length; i++)
                {
                    String typ = record[i].type;
                    if(typ == "null") f_table.writeByte(0);
                    else if(typ == "tinyint") f_table.writeByte(1);
                    else if(typ == "smallint") f_table.writeByte(2);
                    else if(typ == "int") f_table.writeByte(3);
                    else if(typ == "bigint") f_table.writeByte(4);
                    else if(typ == "long") f_table.writeByte(4);
                    else if(typ == "float") f_table.writeByte(5);
                    else if(typ == "double") f_table.writeByte(6);
                    else if(typ == "year") f_table.writeByte(8);
                    else if(typ == "time") f_table.writeByte(9);
                    else if(typ == "datetime") f_table.writeByte(10);
                    else if(typ == "date") f_table.writeByte(11);
                    else if(typ == "text") f_table.writeByte((12 + record[i].length));
                    else System.out.print("");
                }
                for(int i =0; i<record.length; i++){
                    String typ = record[i].type;
                    if(typ == "tinyint") f_table.writeByte(Byte.parseByte(record[i].value));
                    else if(typ == "smallint") f_table.writeShort(Short.parseShort(record[i].value));
                    else if(typ == "int")f_table.writeInt(Integer.parseInt(record[i].value));
                    else if(typ == "bigint")f_table.writeLong(Long.parseLong(record[i].value));
                    else if(typ == "long") f_table.writeLong(Long.parseLong(record[i].value));
                    else if(typ == "float") f_table.writeFloat(Float.parseFloat(record[i].value));
                    else if(typ == "double") f_table.writeDouble(Double.parseDouble(record[i].value));
                    else if(typ == "year") f_table.writeByte(Byte.parseByte(record[i].value));
                    else if(typ == "time") f_table.writeInt(Integer.parseInt(record[i].value));
                    else if(typ == "datetime") f_table.writeLong(Long.parseLong(record[i].value));
                    else if(typ == "date") f_table.writeBytes(record[i].value);
                    else if(typ == "text") f_table.writeBytes(record[i].value);
                    else System.out.print("");
                }
            }
            System.out.print("\nRecord(s) Updated!!");
            f_table.close();
        }
        catch(IOException e) {}
    }
}

class Column
{
    String table;
    String name;
    String type;
    String isnull;
    byte ordinal;
    Column(String table,String name, String type, String isnull, int ordinal)
    {
        this.table = table;
        this.name = name;
        this.type = type;
        this.isnull = isnull;
        this.ordinal = (byte)ordinal ;
    }

}

class Update
{
    String value;
    String type;
    int length;
    int ordinal;
    Update(String type, String value, byte ordinal){
        this.type = type;
        this.value = value;
        this.ordinal = ordinal & 0xFF;
    }
    Update(String type, int length){
        this.type = type;
        this.length = length;
    }
}

