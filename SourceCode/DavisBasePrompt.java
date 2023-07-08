
// Created date : 11/23/2022
// Description : Prompt statements
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import static java.lang.System.out;

public class DavisBasePrompt {

	static String prompt = "davisql> ";
	static String version = "v1.2";
	static String copyright = "Â©2020 Chris Irwin Davis";
	//static boolean isExit = false;
	static boolean isExit = false;

	static long pageSize = 512; 

	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    public static void main(String[] args) 
	{

		splashScreen();

		String userCommand = ""; 
		DavisBaseInitialization.initializeDataStore();
		
		while(!isExit) {
			System.out.print(prompt);
			
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");
	}


	public static void splashScreen() {
		
		//System.out.println(printSeparator("-",80));
		
		System.out.println(printSeparator("-",80));
	    System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		//System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(printSeparator("-",80));
		
	}
	

	public static String printSeparator(String t,int n) 
	{
		String a = "";
		for(int i=0;i<n;i++) {
			a += t;
		}
		return a;
	}
	
	public static void printCmd(String b) {
		System.out.println("\n\t" + b + "\n");
	}
	public static void printDef(String b) {
		System.out.println("\t\t" + b);
	}

	public static void help()
	 {
		out.println(printSeparator("*",80));
		out.println("COMMANDS\n");
		out.println("Note: Commands are case insensitive\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplays table names \n");

		out.println("SELECT <columns> FROM <table_name> [WHERE <condition>];");
		out.println("\tDisplays Table records");
		out.println("DROP TABLE <table_name>;");
		out.println("\tThis command is to drop table data along with its schema\n");
		out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition> is\n");
		out.println("VERSION;");
		out.println("\tDisplays version.\n");
		out.println("HELP;");
		out.println("\tDisplays the help information.\n");
		out.println("EXIT;");
		out.println("\tExits the program.\n");
		out.println(printSeparator("*",80));
	}

	public static String getVersion() {
		return version;
	}
	
/* 	public static String getCopyright() {
		return copyright;
	} */
	
	public static void displayVersion() {
		System.out.println(" Version " + getVersion());
		//System.out.println(getCopyright());
	}
		
	public static void parseUserCommand (String userCommand)
	 {

		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		switch (commandTokens.get(0)) {
			case "create":

				DavisBaseCreateTable.parse_CreateTable(userCommand);
				break;
			case "drop":

				dropTable(userCommand);
				break;
			case "show":
	
				DavisBaseShowTables.parseShow(userCommand);
				break;
			case "insert":

				DavisBaseInsert.parseInsert(userCommand);
				break;
			case "delete":

				DavisBaseDeleteRecord.deleteRecord(userCommand);
				break;
			case "update":
	
				DavisBaseUpdateRecord.updateRecord(userCommand);
				break;
			case "select":

				DavisBaseSelect.parse_query(userCommand);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
			case "quit":
				isExit = true;
				break;
			case "hexdump":
				DavisBaseHexDump.parsingdump(userCommand);
				break;
			default:
				System.out.println("Didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}

	public static void dropTable(String dropTableString) {
		ArrayList<String> cmdTokens = new ArrayList<String>(Arrays.asList(dropTableString.split("\s+")));
		DavisBaseUtils.trimArrayFields(cmdTokens);

		if (cmdTokens.size() != 3 || (!cmdTokens.get(1).equals("table"))) {
			System.out.println(" drop command is invalid");
			System.out.println("USAGE:    drop table <table_name>;");
		} else {
			String tableName = cmdTokens.get(2);
			DavisBaseDropTable.drop_Table(tableName);
		}
	}
}
