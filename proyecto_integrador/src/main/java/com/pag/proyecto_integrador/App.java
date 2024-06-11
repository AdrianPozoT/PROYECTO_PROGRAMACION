package com.pag.proyecto_integrador;



import com.google.protobuf.TextFormat.ParseException;
import com.opencsv.CSVReader;

import com.opencsv.exceptions.CsvValidationException;

import java.io.CharConversionException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Date;
import java.io.FileWriter;
import java.io.FileReader;



public class App 
{
	
	
	public static void circuits() throws CsvValidationException {
	    String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
	    String username = "root";
	    String password = "1111111";
	    
	    String path = "C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\data_source\\circuits.csv";
	   
	    Connection connection = null;
	    
	    try {
	        connection = DriverManager.getConnection(jdbcURL, username, password);
	        connection.setAutoCommit(false);

	        String sql = "INSERT INTO circuits (circuitId, circuitRef, name, location, country, lat, lng, alt, url) " +
	                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
	                     "ON DUPLICATE KEY UPDATE circuitRef=VALUES(circuitRef), name=VALUES(name), location=VALUES(location), country=VALUES(country), lat=VALUES(lat), lng=VALUES(lng), alt=VALUES(alt), url=VALUES(url)";
	        PreparedStatement statement = connection.prepareStatement(sql);

	        CSVReader reader = new CSVReader(new FileReader(path));
	        String[] nextLine;
	        reader.readNext(); // Saltar la cabecera

	        List<JSONObject> jsonObjects = new ArrayList<>();

	        while ((nextLine = reader.readNext()) != null) {
	            int circuitId = Integer.parseInt(nextLine[0]);
	            String circuitRef = nextLine[1];
	            String name = nextLine[2];
	            String location = nextLine[3];
	            String country = nextLine[4];
	            Float lat = Float.parseFloat(nextLine[5]);
	            Float lng = Float.parseFloat(nextLine[6]);
	            Float alt = Float.parseFloat(nextLine[7]);
	            String url = nextLine[8];

	            statement.setInt(1, circuitId);
	            statement.setString(2, circuitRef);
	            statement.setString(3, name);
	            statement.setString(4, location);
	            statement.setString(5, country);
	            statement.setFloat(6, lat);
	            statement.setFloat(7, lng);
	            statement.setFloat(8, alt);
	            statement.setString(9, url);

	            statement.addBatch();

	            // Crear objeto JSON
	            JSONObject circuitJson = new JSONObject();
	            circuitJson.put("circuitId", circuitId);
	            circuitJson.put("circuitRef", circuitRef);
	            circuitJson.put("name", name);
	            circuitJson.put("location", location);
	            circuitJson.put("country", country);
	            circuitJson.put("lat", lat);
	            circuitJson.put("lng", lng);
	            circuitJson.put("alt", alt);
	            circuitJson.put("url", url);

	            jsonObjects.add(circuitJson);
	        }

	        statement.executeBatch();
	        connection.commit();

	        // Escribir datos JSON a un archivo
	        JSONArray jsonArray = new JSONArray(jsonObjects);
	        try (FileWriter file = new FileWriter("C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\Json\\circuits.json")) {
	            file.write(jsonArray.toString(4)); // Indentación de 4 espacios
	        }

	        System.out.println("Datos insertados y registrados en JSON correctamente.");
	    } catch (Exception e) {
	        if (connection != null) {
	            try {
	                connection.rollback();
	            } catch (SQLException ex) {
	                ex.printStackTrace();
	            }
	        }
	        e.printStackTrace();
	    } finally {
	        if (connection != null) {
	            try {
	                connection.close();
	            } catch (SQLException ex) {
	                ex.printStackTrace();
	            }
	        }
	    }
	}
	
	public static void seasons() throws CsvValidationException {
	    String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
	    String username = "root";
	    String password = "1111111";
	    
	    String path = "C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\CSV's\\seasons.csv";
	   
	    Connection connection = null;
	    
	    try {
	        connection = DriverManager.getConnection(jdbcURL, username, password);
	        connection.setAutoCommit(false);

	        String sql = "INSERT INTO seasons (year, url) VALUES (?, ?) ON DUPLICATE KEY UPDATE url=VALUES(url)";
	        PreparedStatement statement = connection.prepareStatement(sql);

	        CSVReader reader = new CSVReader(new FileReader(path));
	        String[] nextLine;
	        reader.readNext(); // Saltar la cabecera

	        List<JSONObject> jsonObjects = new ArrayList<>();

	        while ((nextLine = reader.readNext()) != null) {
	            int year = Integer.parseInt(nextLine[0]);
	            String url = nextLine[1];

	            statement.setInt(1, year);
	            statement.setString(2, url);

	            statement.addBatch();

	            // Crear objeto JSON
	            JSONObject seasonJson = new JSONObject();
	            seasonJson.put("year", year);
	            seasonJson.put("url", url);

	            jsonObjects.add(seasonJson);
	        }

	        statement.executeBatch();
	        connection.commit();

	        // Escribir datos JSON a un archivo
	        JSONArray jsonArray = new JSONArray(jsonObjects);
	        try (FileWriter file = new FileWriter("C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\Json\\seasons.json")) {
	            file.write(jsonArray.toString(4)); // Indentación de 4 espacios
	        }

	        System.out.println("Datos insertados y registrados en JSON correctamente.");
	    } catch (Exception e) {
	        if (connection != null) {
	            try {
	                connection.rollback();
	            } catch (SQLException ex) {
	                ex.printStackTrace();
	            }
	        }
	        e.printStackTrace();
	    } finally {
	        if (connection != null) {
	            try {
	                connection.close();
	            } catch (SQLException ex) {
	                ex.printStackTrace();
	            }
	        }
	    }
	}


	
	public static void races() throws CsvValidationException {
	    String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
	    String username = "root";
	    String password = "1111111";
	    
	    String path = "C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\data_source\\races.csv";
	   
	    Connection connection = null;
	    
	    try {
	        connection = DriverManager.getConnection(jdbcURL, username, password);
	        connection.setAutoCommit(false);

	        String sql = "INSERT INTO races (raceId, year, round, circuitId, name, date, time, url, fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time, quali_date, quali_time, sprint_date, sprint_time) " +
	                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
	                     "ON DUPLICATE KEY UPDATE year=VALUES(year), round=VALUES(round), circuitId=VALUES(circuitId), name=VALUES(name), date=VALUES(date), time=VALUES(time), url=VALUES(url), fp1_date=VALUES(fp1_date), fp1_time=VALUES(fp1_time), fp2_date=VALUES(fp2_date), fp2_time=VALUES(fp2_time), fp3_date=VALUES(fp3_date), fp3_time=VALUES(fp3_time), quali_date=VALUES(quali_date), quali_time=VALUES(quali_time), sprint_date=VALUES(sprint_date), sprint_time=VALUES(sprint_time)";
	        PreparedStatement statement = connection.prepareStatement(sql);

	        CSVReader reader = new CSVReader(new FileReader(path));
	        String[] nextLine;
	        reader.readNext(); // Saltar la cabecera

	        List<JSONObject> jsonObjects = new ArrayList<>();

	        while ((nextLine = reader.readNext()) != null) {
                try {
                    int raceId = parseInteger(nextLine[0]);
                    int year = parseInteger(nextLine[1]);
                    int round = parseInteger(nextLine[2]);
                    int circuitId = parseInteger(nextLine[3]);
                    String name = parseString(nextLine[4]);
                    Date date = parseDate(nextLine[5]);
                    Time time = parseTime(nextLine[6]);
                    String url = parseString(nextLine[7]);
                    Date fp1_date = parseDate(nextLine[8]);
                    Time fp1_time = parseTime(nextLine[9]);
                    Date fp2_date = parseDate(nextLine[10]);
                    Time fp2_time = parseTime(nextLine[11]);
                    Date fp3_date = parseDate(nextLine[12]);
                    Time fp3_time = parseTime(nextLine[13]);
                    Date quali_date = parseDate(nextLine[14]);
                    Time quali_time = parseTime(nextLine[15]);
                    Date sprint_date = parseDate(nextLine[16]);
                    Time sprint_time = parseTime(nextLine[17]);

                    statement.setObject(1, raceId);
                    statement.setObject(2, year);
                    statement.setObject(3, round);
                    statement.setObject(4, circuitId);
                    statement.setObject(5, name);
                    statement.setObject(6, date);
                    statement.setObject(7, time);
                    statement.setObject(8, url);
                    statement.setObject(9, fp1_date);
                    statement.setObject(10, fp1_time);
                    statement.setObject(11, fp2_date);
                    statement.setObject(12, fp2_time);
                    statement.setObject(13, fp3_date);
                    statement.setObject(14, fp3_time);
                    statement.setObject(15, quali_date);
                    statement.setObject(16, quali_time);
                    statement.setObject(17, sprint_date);
                    statement.setObject(18, sprint_time);

                    statement.addBatch();

                    // Crear objeto JSON
                    JSONObject raceJson = new JSONObject();
                    raceJson.put("raceId", raceId);
                    raceJson.put("year", year);
                    raceJson.put("round", round);
                    raceJson.put("circuitId", circuitId);
                    raceJson.put("name", name);
                    raceJson.put("date", date != null ? date.toString() : null);
                    raceJson.put("time", time != null ? time.toString() : null);
                    raceJson.put("url", url);
                    raceJson.put("fp1_date", fp1_date != null ? fp1_date.toString() : null);
                    raceJson.put("fp1_time", fp1_time != null ? fp1_time.toString() : null);
                    raceJson.put("fp2_date", fp2_date != null ? fp2_date.toString() : null);
                    raceJson.put("fp2_time", fp2_time != null ? fp2_time.toString() : null);
                    raceJson.put("fp3_date", fp3_date != null ? fp3_date.toString() : null);
                    raceJson.put("fp3_time", fp3_time != null ? fp3_time.toString() : null);
                    raceJson.put("quali_date", quali_date != null ? quali_date.toString() : null);
                    raceJson.put("quali_time", quali_time != null ? quali_time.toString() : null);
                    raceJson.put("sprint_date", sprint_date != null ? sprint_date.toString() : null);
                    raceJson.put("sprint_time", sprint_time != null ? sprint_time.toString() : null);

                    jsonObjects.add(raceJson);
                 } catch (IllegalArgumentException e) {
                     System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
                 }
             }

	        statement.executeBatch(); // Ejecutar todas las instrucciones en el lote

             // Escribir datos JSON a un archivo
             JSONArray jsonArray = new JSONArray(jsonObjects);
             try (FileWriter file = new FileWriter("C:\\\\INSTALADORES\\\\a_ProyectoProgramacion__3_S\\\\ACTUAL\\\\proyecto_integrador\\\\proyecto_integrador\\Json\\races.json")) {
                 file.write(jsonArray.toString(4)); // Indentación de 4 espacios
             }

             System.out.println("Datos insertados y registrados en JSON correctamente.");
         } catch (IOException | SQLException e) {
             e.printStackTrace();
             try {
                 if (connection != null) {
                     connection.rollback();
                 }
             } catch (SQLException ex) {
                 ex.printStackTrace();
             }
         } finally {
             try {
                 if (connection != null) connection.close();
             } catch (SQLException ex) {
                 ex.printStackTrace();
             }
         }
 }

	
	
	/*
	public static void races() throws CsvValidationException  {
		String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
        String username = "root";
        String password = "1111111";
        
        
    	String path = "C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\data_source\\races.csv";
        
    	Connection connection = null;
    	
    	try {
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO race (raceId, year, round, circuitId, name, date, time, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            CSVReader reader = new CSVReader(new FileReader(path));
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            while ((nextLine = reader.readNext()) != null) {
            	int raceId = Integer.parseInt(nextLine[0]);
            	int year = Integer.parseInt(nextLine[1]);
            	int round = Integer.parseInt(nextLine[2]);
            	int circuitId = Integer.parseInt(nextLine[3]);
                String name = nextLine[4];
                String date = nextLine[5];
                String time = nextLine[6];
                String url = nextLine[7];

                statement.setInt(1, raceId );
                statement.setInt(2, year);
                statement.setInt(3, round);
                statement.setInt(4, circuitId);
                statement.setString(5, name);
                statement.setString(6, date);
                statement.setString(7, time);
                statement.setString(8, url);
                


                statement.addBatch();
            }

            reader.close();
            statement.executeBatch();
            connection.commit();
            statement.close();
            connection.close();

            System.out.println("Datos insertados correctamente.");

        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
	}
	*/
    private static Integer parseInteger(String value) {
        return "N".equals(value) ? null : Integer.parseInt(value);
    }

    private static String parseString(String value) {
        return "N".equals(value) ? null : value;
    }

    private static Date parseDate(String value) {
        return "N".equals(value) ? null : Date.valueOf(value);
    }

    private static Time parseTime(String value) {
        return "N".equals(value) ? null : Time.valueOf(value);
    }
    
    private static Float parseFloat(String value) {
        return "N".equals(value) ? null : Float.parseFloat(value);
    }
	
    public static void results() throws CsvValidationException {
        String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
        String username = "root";
        String password = "1111111";
        
        String path = "C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\data_source\\results.csv";
       
        Connection connection = null;
        
        try {
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO results (resultId, raceId, driverId, constructorId, number, grid, position, positionText, positionOrder, points, laps, time, miliseconds, fastestLap, rank, fastestLapTime, fastestLapSpeed, statusId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE raceId=VALUES(raceId), driverId=VALUES(driverId), constructorId=VALUES(constructorId), number=VALUES(number), grid=VALUES(grid), position=VALUES(position), positionText=VALUES(positionText), positionOrder=VALUES(positionOrder), points=VALUES(points), laps=VALUES(laps), time=VALUES(time), miliseconds=VALUES(miliseconds), fastestLap=VALUES(fastestLap), rank=VALUES(rank), fastestLapTime=VALUES(fastestLapTime), fastestLapSpeed=VALUES(fastestLapSpeed), statusId=VALUES(statusId)";
            PreparedStatement statement = connection.prepareStatement(sql);

            CSVReader reader = new CSVReader(new FileReader(path));
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera
            
            List<JSONObject> jsonObjects = new ArrayList<>();

            while ((nextLine = reader.readNext()) != null) {
                try {
                    Integer driverId = parseInteger(nextLine[0]);
                    String driverRef = nextLine[1];
                    Integer number = parseInteger(nextLine[2]);
                    String code = nextLine[3];
                    String forename = nextLine[4];
                    String surname = nextLine[5];
                    Date dob = parseDate(nextLine[6]);
                    String nationality = nextLine[7];
                    String url = nextLine[8];

                    if (driverId != null) {
                        statement.setInt(1, driverId);
                    } else {
                    	statement.setNull(1, java.sql.Types.INTEGER);
                    }

                    statement.setString(2, driverRef);

                    if (number != null) {
                    	statement.setInt(3, number);
                    } else {
                    	statement.setNull(3, java.sql.Types.INTEGER);
                    }

                    statement.setString(4, code);
                    statement.setString(5, forename);
                    statement.setString(6, surname);
                    statement.setDate(7, dob);
                    statement.setString(8, nationality);
                    statement.setString(9, url);

                    statement.addBatch();

                    // Crear objeto JSON
                    JSONObject driverJson = new JSONObject();
                    driverJson.put("driverId", driverId);
                    driverJson.put("driverRef", driverRef);
                    driverJson.put("number", number);
                    driverJson.put("code", code);
                    driverJson.put("forename", forename);
                    driverJson.put("surname", surname);
                    driverJson.put("dob", dob.toString());
                    driverJson.put("nationality", nationality);
                    driverJson.put("url", url);

                    jsonObjects.add(driverJson);
                } catch (IllegalArgumentException e) {
                    System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
                }
            }

            statement.executeBatch(); // Ejecutar todas las instrucciones en el lote

            // Escribir datos JSON a un archivo
            JSONArray jsonArray = new JSONArray(jsonObjects);
            try (FileWriter file = new FileWriter("C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\Json\\drivers.json")) {
                file.write(jsonArray.toString(4)); // Indentación de 4 espacios
            }

            System.out.println("Datos insertados y registrados en JSON correctamente.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (connection != null) connection.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

	
	
	public static void drivers() throws CsvValidationException{
	    String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
	    String username = "root";
	    String password = "1111111";
	    
	    String path = "C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\data_source\\drivers.csv";
	   
	    Connection connection = null;
	    
	    try {
	        connection = DriverManager.getConnection(jdbcURL, username, password);
	        connection.setAutoCommit(false);

	        String sql = "INSERT INTO drivers (driverId, driverRef, number, code, forename, surname, dob, nationality, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE driverRef=VALUES(driverRef), number=VALUES(number), code=VALUES(code), forename=VALUES(forename), surname=VALUES(surname), dob=VALUES(dob), nationality=VALUES(nationality), url=VALUES(url)";
	        PreparedStatement statement = connection.prepareStatement(sql);

	        CSVReader reader = new CSVReader(new FileReader(path));
	        String[] nextLine;
	        reader.readNext(); // Saltar la cabecera

	        List<JSONObject> jsonObjects = new ArrayList<>();

	        while ((nextLine = reader.readNext()) != null) {
	            try {
	                Integer driverId = parseInteger(nextLine[0]);
	                String driverRef = nextLine[1];
	                Integer number = parseInteger(nextLine[2]);
	                String code = nextLine[3];
	                String forename = nextLine[4];
	                String surname = nextLine[5];
	                Date dob = parseDate(nextLine[6]);
	                String nationality = nextLine[7];
	                String url = nextLine[8];

	                if (driverId != null) {
	                    statement.setInt(1, driverId);
	                } else {
	                    statement.setNull(1, java.sql.Types.INTEGER);
	                }

	                statement.setString(2, driverRef);

	                if (number != null) {
	                    statement.setInt(3, number);
	                } else {
	                    statement.setNull(3, java.sql.Types.INTEGER);
	                }

	                statement.setString(4, code);
	                statement.setString(5, forename);
	                statement.setString(6, surname);
	                if (dob != null) {
	                    statement.setDate(7, new java.sql.Date(dob.getTime()));
	                } else {
	                    statement.setNull(7, java.sql.Types.DATE);
	                }
	                statement.setString(8, nationality);
	                statement.setString(9, url);

	                statement.addBatch();

	                // Crear objeto JSON
	                JSONObject driverJson = new JSONObject();
	                driverJson.put("driverId", driverId);
	                driverJson.put("driverRef", driverRef);
	                driverJson.put("number", number);
	                driverJson.put("code", code);
	                driverJson.put("forename", forename);
	                driverJson.put("surname", surname);
	                driverJson.put("dob", dob != null ? dob.toString() : null);
	                driverJson.put("nationality", nationality);
	                driverJson.put("url", url);

	                jsonObjects.add(driverJson);
	            } catch (IllegalArgumentException e) {
	                System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
	            }
	        }

	        statement.executeBatch();
	        connection.commit();

	        // Escribir datos JSON a un archivo
	        JSONArray jsonArray = new JSONArray(jsonObjects);
	        try (FileWriter file = new FileWriter("C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\Json\\drivers.json")) {
	            file.write(jsonArray.toString(4)); // Indentación de 4 espacios
	        }

	        System.out.println("Datos insertados y registrados en JSON correctamente.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (connection != null) connection.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
	}


	public static void constructors() throws CsvValidationException {
	    String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
	    String username = "root";
	    String password = "1111111";
	    
	    String path = "C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\data_source\\constructors.csv";
	   
	    Connection connection = null;
	    
	    try {
	        connection = DriverManager.getConnection(jdbcURL, username, password);
	        connection.setAutoCommit(false);

	        String sql = "INSERT INTO constructors (constructorId, constructorRef, name, nationality, url) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE constructorRef=VALUES(constructorRef), name=VALUES(name), nationality=VALUES(nationality), url=VALUES(url)";
	        PreparedStatement statement = connection.prepareStatement(sql);

	        CSVReader reader = new CSVReader(new FileReader(path));
	        String[] nextLine;
	        reader.readNext(); // Saltar la cabecera

	        List<JSONObject> jsonObjects = new ArrayList<>();

	        while ((nextLine = reader.readNext()) != null) {
	            int constructorId = parseInteger(nextLine[0]);
	            String constructorRef = nextLine[1];
	            String name = nextLine[2];
	            String nationality = nextLine[3];
	            String url = nextLine[4];

	            statement.setInt(1, constructorId);
	            statement.setString(2, constructorRef);
	            statement.setString(3, name);
	            statement.setString(4, nationality);
	            statement.setString(5, url);

	            statement.addBatch();

	            // Crear objeto JSON
	            JSONObject constructorJson = new JSONObject();
	            constructorJson.put("constructorId", constructorId);
	            constructorJson.put("constructorRef", constructorRef);
	            constructorJson.put("name", name);
	            constructorJson.put("nationality", nationality);
	            constructorJson.put("url", url);

	            jsonObjects.add(constructorJson);
	        }

	        statement.executeBatch();
	        connection.commit();

	        // Escribir datos JSON a un archivo
	        JSONArray jsonArray = new JSONArray(jsonObjects);
	        try (FileWriter file = new FileWriter("C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\Json\\constructors.json")) {
	            file.write(jsonArray.toString(4)); // Indentación de 4 espacios
	        }

	        System.out.println("Datos insertados y registrados en JSON correctamente.");
	    } catch (Exception e) {
	        if (connection != null) {
	            try {
	                connection.rollback();
	            } catch (SQLException ex) {
	                ex.printStackTrace();
	            }
	        }
	        e.printStackTrace();
	    } finally {
	        if (connection != null) {
	            try {
	                connection.close();
	            } catch (SQLException ex) {
	                ex.printStackTrace();
	            }
	        }
	    }
	}

    
	
	public static void lap_times() throws CsvValidationException {
	    String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
	    String username = "root";
	    String password = "1111111";
	    
	    String path = "C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\data_source\\lap_times.csv";
	   
	    Connection connection = null;
	    
	    try {
	        connection = DriverManager.getConnection(jdbcURL, username, password);
	        connection.setAutoCommit(false);

	        String sql = "INSERT INTO lap_times (raceId, driverId, lap, position, time, miliseconds) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE position=VALUES(position), time=VALUES(time), miliseconds=VALUES(miliseconds)";
	        PreparedStatement statement = connection.prepareStatement(sql);

	        CSVReader reader = new CSVReader(new FileReader(path));
	        String[] nextLine;
	        reader.readNext(); // Saltar la cabecera

	        List<JSONObject> jsonObjects = new ArrayList<>();

	        while ((nextLine = reader.readNext()) != null) {
	            int raceId = Integer.parseInt(nextLine[0]);
	            int driverId = Integer.parseInt(nextLine[1]);
	            int lap = Integer.parseInt(nextLine[2]);
	            int position = Integer.parseInt(nextLine[3]);
	            String time = nextLine[4];
	            int miliseconds = Integer.parseInt(nextLine[5]);

	            statement.setInt(1, raceId);
	            statement.setInt(2, driverId);
	            statement.setInt(3, lap);
	            statement.setInt(4, position);
	            statement.setString(5, time);
	            statement.setInt(6, miliseconds);

	            statement.addBatch();

	            // Crear objeto JSON
	            JSONObject lapTimeJson = new JSONObject();
	            lapTimeJson.put("raceId", raceId);
	            lapTimeJson.put("driverId", driverId);
	            lapTimeJson.put("lap", lap);
	            lapTimeJson.put("position", position);
	            lapTimeJson.put("time", time);
	            lapTimeJson.put("miliseconds", miliseconds);

	            jsonObjects.add(lapTimeJson);
	        }

	        statement.executeBatch();
	        connection.commit();

	        // Escribir datos JSON a un archivo
	        JSONArray jsonArray = new JSONArray(jsonObjects);
	        try (FileWriter file = new FileWriter("C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\Json\\lap_times.json")) {
	            file.write(jsonArray.toString(4)); // Indentación de 4 espacios
	        }

	        System.out.println("Datos insertados y registrados en JSON correctamente.");
	    } catch (Exception e) {
	        if (connection != null) {
	            try {
	                connection.rollback();
	            } catch (SQLException ex) {
	                ex.printStackTrace();
	            }
	        }
	        e.printStackTrace();
	    } finally {
	        if (connection != null) {
	            try {
	                connection.close();
	            } catch (SQLException ex) {
	                ex.printStackTrace();
	            }
	        }
	    }
	}

	
	public static void pit_stops() throws CsvValidationException {
	    String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
	    String username = "root";
	    String password = "1111111";
	    
	    String path = "C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\data_source\\pit_stops.csv";
	   
	    Connection connection = null;
	    
	    try {
	        connection = DriverManager.getConnection(jdbcURL, username, password);
	        connection.setAutoCommit(false);

	        String sql = "INSERT INTO pit_stops (raceId, driverId, stop, lap, time, duration, miliseconds) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE lap=VALUES(lap), time=VALUES(time), duration=VALUES(duration), miliseconds=VALUES(miliseconds)";
	        PreparedStatement statement = connection.prepareStatement(sql);

	        CSVReader reader = new CSVReader(new FileReader(path));
	        String[] nextLine;
	        reader.readNext(); // Saltar la cabecera

	        List<JSONObject> jsonObjects = new ArrayList<>();

	        while ((nextLine = reader.readNext()) != null) {
	            int raceId = Integer.parseInt(nextLine[0]);
	            int driverId = Integer.parseInt(nextLine[1]);
	            int stop = Integer.parseInt(nextLine[2]);
	            int lap = Integer.parseInt(nextLine[3]);
	            String time = nextLine[4];
	            String duration = nextLine[5];
	            int miliseconds = Integer.parseInt(nextLine[6]);

	            statement.setInt(1, raceId);
	            statement.setInt(2, driverId);
	            statement.setInt(3, stop);
	            statement.setInt(4, lap);
	            statement.setString(5, time);
	            statement.setString(6, duration);
	            statement.setInt(7, miliseconds);

	            statement.addBatch();

	            // Crear objeto JSON
	            JSONObject pitStopJson = new JSONObject();
	            pitStopJson.put("raceId", raceId);
	            pitStopJson.put("driverId", driverId);
	            pitStopJson.put("stop", stop);
	            pitStopJson.put("lap", lap);
	            pitStopJson.put("time", time);
	            pitStopJson.put("duration", duration);
	            pitStopJson.put("miliseconds", miliseconds);

	            jsonObjects.add(pitStopJson);
	        }

	        statement.executeBatch();
	        connection.commit();

	        // Escribir datos JSON a un archivo
	        JSONArray jsonArray = new JSONArray(jsonObjects);
	        try (FileWriter file = new FileWriter("C:\\INSTALADORES\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\Json\\pit_stops.json")) {
	            file.write(jsonArray.toString(4)); // Indentación de 4 espacios
	        }

	        System.out.println("Datos insertados y registrados en JSON correctamente.");
	    } catch (Exception e) {
	        if (connection != null) {
	            try {
	                connection.rollback();
	            } catch (SQLException ex) {
	                ex.printStackTrace();
	            }
	        }
	        e.printStackTrace();
	    } finally {
	        if (connection != null) {
	            try {
	                connection.close();
	            } catch (SQLException ex) {
	                ex.printStackTrace();
	            }
	        }
	    }
	}

	
    public static void qualifying() throws CsvValidationException {
		String jdbcURL = "jdbc:mysql://127.0.0.1/formula1?serverTimezone=UTC";
        String username = "root";
        String password = "1111111";

	    Connection connection = null;
      
		if (connection == null) {
            System.out.println("La conexión a la base de datos no se pudo establecer.");
            return;
        }

        String path = "C:\\INSTALADORES\\\\a_ProyectoProgramacion__3_S\\ACTUAL\\proyecto_integrador\\proyecto_integrador\\data_source\\drivers.json";

        try (
            CSVReader reader = new CSVReader(new FileReader(path));
            PreparedStatement pst = connection.prepareStatement(
                "INSERT INTO qualifying (qualifyId, raceId, driverId, constructorId, number, position, q1, q2, q3) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE raceId=VALUES(raceId), driverId=VALUES(driverId), constructorId=VALUES(constructorId), number=VALUES(number), position=VALUES(position), q1=VALUES(q1), q2=VALUES(q2), q3=VALUES(q3)")
        ) {
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            List<JSONObject> jsonObjects = new ArrayList<>();

            while ((nextLine = reader.readNext()) != null) {
                try {
                    int qualifyId = parseInteger(nextLine[0]);
                    int raceId = parseInteger(nextLine[1]);
                    int driverId = parseInteger(nextLine[2]);
                    int constructorId = parseInteger(nextLine[3]);
                    int number = parseInteger(nextLine[4]);
                    int position = parseInteger(nextLine[5]);
                    String q1 = nextLine[6];
                    String q2 = nextLine[7];
                    String q3 = nextLine[8];

                    pst.setInt(1, qualifyId);
                    pst.setInt(2, raceId);
                    pst.setInt(3, driverId);
                    pst.setInt(4, constructorId);
                    pst.setInt(5, number);
                    pst.setInt(6, position);
                    pst.setString(7, q1);
                    pst.setString(8, q2);
                    pst.setString(9, q3);

                    pst.addBatch();

                    // Crear objeto JSON
                    JSONObject qualifyingJson = new JSONObject();
                    qualifyingJson.put("qualifyId", qualifyId);
                    qualifyingJson.put("raceId", raceId);
                    qualifyingJson.put("driverId", driverId);
                    qualifyingJson.put("constructorId", constructorId);
                    qualifyingJson.put("number", number);
                    qualifyingJson.put("position", position);
                    qualifyingJson.put("q1", q1);
                    qualifyingJson.put("q2", q2);
                    qualifyingJson.put("q3", q3);

                    jsonObjects.add(qualifyingJson);
                } catch (IllegalArgumentException e) {
                    System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
                }
            }

            pst.executeBatch(); // Ejecutar todas las instrucciones en el lote

            // Escribir datos JSON a un archivo
            JSONArray jsonArray = new JSONArray(jsonObjects);
            try (FileWriter file = new FileWriter("C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\Json\\qualifying.json")) {
                file.write(jsonArray.toString(4)); // Indentación de 4 espacios
            }

            System.out.println("Datos insertados y registrados en JSON correctamente.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (connection != null) connection.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

	

	public static void main( String[] args ) throws CsvValidationException
    {
    	
    	circuits();
    	races();
    	results();
    	drivers();
    	constructors();
    	lap_times();
    	pit_stops();
    	qualifying();
    	
//    	delete from circuits;
//    	select count(*) from circuits;
//    	select * from circuits;
    	
    	
    }
}
