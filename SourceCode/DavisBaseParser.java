
// Created date : 11/23/2022
// Description : Parser 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.nio.ByteBuffer;

public class DavisBaseParser {

  private static final String variableRegex = "[a-zA-Z_][a-zA-Z0-9_]*";
  private static final Set<String> allowedTypes;

  static {
    allowedTypes = new HashSet<>();
    allowedTypes.add("tinyint");
    allowedTypes.add("tinyint");
    allowedTypes.add("smallint");
    allowedTypes.add("int");
    allowedTypes.add("bigint");
    allowedTypes.add("float");
    allowedTypes.add("double");
    allowedTypes.add("date");
    allowedTypes.add("time");
    allowedTypes.add("datetime");
    allowedTypes.add("text");
  }

  private static String fixDate(String dateString) throws Exception {
    if (dateString.length() != 10) {
      throw new Exception("Error: Invalid date string");
    }
    String replaced = dateString.replaceAll("/", "-");
    Pattern pattern = Pattern.compile("(\\d{2})([-/])(\\d{2})\\2(\\d{4})");
    Matcher matcher = pattern.matcher(replaced);

    if (!matcher.find()) {
      return replaced;
    }
    String m = matcher.group(1);
    String d = matcher.group(3);
    String y = matcher.group(4);
    return y + "-" + m + "-" + d;
  }
  private static String fixDateTime(String dateTimeString) throws Exception {
    try {
      if (dateTimeString.length() != 19) {
        throw new Exception("");
      }
      String dateString = dateTimeString.substring(0, 10);
      return fixDate(dateString) + dateTimeString.substring(10);
    } catch (Exception e) {
      throw new Exception("Error: Invalid date time string");
    }
  }
  public static Long parseDate(String value) throws Exception {
    LocalDate date = LocalDate.parse(fixDate(value));
    ZoneId zone = ZoneId.systemDefault();
    return date.atStartOfDay(zone).toEpochSecond();
  }
  public static Long parseDateTime(String value) throws Exception {
    LocalDateTime dateTime = LocalDateTime.parse(fixDateTime(value));
    ZoneId zone = ZoneId.systemDefault();
    return dateTime.atZone(zone).toEpochSecond();
  }
  public static Integer parseTime(String value) throws Exception {
    LocalTime time = LocalTime.parse(value);
    return time.toSecondOfDay();
  }

  public static String unparseDate(byte[] value) throws Exception {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    ZoneId zone = ZoneId.systemDefault();
    Instant instant = Instant.ofEpochSecond(buffer.getLong());
    LocalDate date = LocalDate.ofInstant(instant, zone);
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    return date.format(fmt);
  }
  public static String unparseDateTime(byte[] value) throws Exception {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    ZoneId zone = ZoneId.systemDefault();
    Instant instant = Instant.ofEpochSecond(buffer.getLong());
    LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zone);
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    return dateTime.format(fmt);
  }
  public static String unparseTime(byte[] value) throws Exception {
    try {
      ByteBuffer buffer = ByteBuffer.wrap(value);
      ZoneId zone = ZoneId.systemDefault();
      LocalTime time = LocalTime.ofSecondOfDay(buffer.getInt());
      DateTimeFormatter fmt = DateTimeFormatter.ofPattern("HH:mm:ss");
      return time.format(fmt);
    } catch (Exception e) {
      System.out.println(e);
      return "";
    }
  }

  public static void valueToBytes(String value, List<Byte> ValueInBytes, String clmType) {

    DataValue dataValue = new DataValue();

    try {
      switch (clmType) {
        case "tinyint":
          dataValue = new DataValue(DataType.TINYINT.code);
          dataValue.setTinyint((byte)(Integer.valueOf(value) & 0xFF));
          break;
        case "smallint":
          dataValue = new DataValue(DataType.SMALLINT.code);
          dataValue.setSmallint(Short.valueOf(value));
          break;
        case "int":
          dataValue = new DataValue(DataType.INT.code);
          dataValue.setInt(Integer.valueOf(value));
          break;
        case "bigint":
          dataValue = new DataValue(DataType.BIGINT.code);
          dataValue.setBigint(Long.valueOf(value));
          break;
        case "float":
          dataValue = new DataValue(DataType.FLOAT.code);
          dataValue.setFloat(Float.valueOf(value));
          break;
        case "double":
          dataValue = new DataValue(DataType.DOUBLE.code);
          dataValue.setDouble(Double.valueOf(value));
          break;
        case "date":
          dataValue = new DataValue(DataType.DATE.code);
          dataValue.setBigint(parseDate(value));
          break;
        case "time":
          dataValue = new DataValue(DataType.TIME.code);
          dataValue.setInt(parseTime(value));
          break;
        case "datetime":
          dataValue = new DataValue(DataType.DATETIME.code);
          dataValue.setBigint(parseDateTime(value));
          break;
        case "text":
          dataValue = new DataValue(DataType.TEXT.code);
          dataValue.setText(value);
          break;
        default:
          throw new Exception("");
      }
      for (byte b: dataValue.getBytes()) {
        ValueInBytes.add(b);
      }
    } catch (Exception e) {
      System.out.println("Error: Invalid data type");
    }
  }

  public static String parseCreateTable(String statement, ArrayList<ArrayList<String>> attrs) throws Exception {
    statement = statement.replaceAll("\\s+", " ").trim();
    Pattern pattern = Pattern.compile("^create table (" + variableRegex + ")\\s*\\((.*)\\)\\s*;$");
    Matcher matcher = pattern.matcher(statement);

    if (!matcher.find()) {
      throw new Exception("Error: Invalid CREATE TABLE statement");
    }
    Pattern attrPattern = Pattern.compile("^(" + variableRegex + ")\\s+(\\w+)$");
    String tableName = matcher.group(1);
    String attrsString = matcher.group(2);

    for (String attr: attrsString.split(",")) {
      attr = attr.trim();
      Matcher attrMatcher = attrPattern.matcher(attr);

      if (!attrMatcher.find()) {
        throw new Exception("Error: Invalid attribute '" + attr + "' in CREATE TABLE statement");
      }
      String attrName = attrMatcher.group(1);
      String attrType = attrMatcher.group(2);

      if (!allowedTypes.contains(attrType)) {
        throw new Exception("Error: Invalid attribute type '" + attrType + "' in CREATE TABLE statement");
      }
      ArrayList<String> attrResult = new ArrayList<>();
      attrResult.add(attrName);
      attrResult.add(attrType);
      attrs.add(attrResult);
    }
    return tableName;
  }

}
