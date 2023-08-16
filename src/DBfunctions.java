import java.sql.*;
import java.util.Random;

public class DBfunctions {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/test";
        String username = "postgres";
        String password = "root";
        try(Connection connection = DriverManager.getConnection(url,username,password))
        {
            System.out.println("database connected succefully.");
//            dropTable(connection,"employee");
//            createEmployeesTable(connection);
//            for (long i = 0; i < 1000000;i++)
//            {
//                insertEmployee(connection,"Employee " + i, generateRandomNumber(20,40), "Department " + generateRandomNumber(1,5),generateRandomNumber(3000,5000));
//            }
            ResultSet r = retrieveAll(connection,"employee");
            printResultSet(r);

        }
        catch (Exception e)
        {
            System.out.println(e);
        }
    }

    private static void printResultSet(ResultSet r) throws SQLException {
        while(r.next())
        {
            System.out.println(
                    "Id: " + r.getInt("emp_id") +","
                    +" Name: " + r.getString("emp_name") + ","
                    + " Age: " + r.getInt("emp_age") + ","
                    + " Department: " + r.getString("emp_department") +","
                    + " Salary: " + r.getInt("emp_salary")
            );
        }
    }


    private static boolean createEmployeesTable(Connection connection)
    {
        try
        {
            Statement statement = connection.createStatement();
            String createTable = "CREATE TABLE employee (" +
                    "     emp_id SERIAL PRIMARY KEY," +
                    "     emp_name VARCHAR(100) NOT NULL," +
                    "     emp_age INT," +
                    "     emp_department VARCHAR(50)," +
                    "     emp_salary DECIMAL(10, 2)" +
                    ");";
            return statement.execute(createTable);
        }
        catch(Exception e)
        {
            System.out.println(e);
        }
    return false;
    }

    private static int insertEmployee(Connection connection, String name, int age, String department, int salary)
    {
        try
        {
            String query = "INSERT INTO employee (emp_name, emp_age, emp_department, emp_salary) values (?,?,?,?)";
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setString(1,name);
            statement.setInt(2,age);
            statement.setString(3,department);
            statement.setInt(4,salary);
            return statement.executeUpdate();
        }
        catch(Exception e)
        {
            System.out.println(e);
        }
    return 0;
    }

    private static ResultSet retrieveAll(Connection connection, String tableName)
    {
        try
        {
            String query = "SELECT * FROM  "+tableName +";";
            PreparedStatement statement = connection.prepareStatement(query);
            return statement.executeQuery();
        }
        catch(Exception e)
        {
            System.out.println(e);
        }
        return null;
    }

    private static boolean dropTable(Connection connection,String tableName)
    {
        try
        {
            String query = "drop table " + tableName;
            PreparedStatement statement = connection.prepareStatement(query);
            statement.executeUpdate();
            return true;

        }
        catch(Exception e)
        {
            System.out.println(e);
        }
        return false;

    }

    private static int generateRandomNumber(int min,int max)
    {
        Random r = new Random();
        return r.nextInt(max-min) + min;
    }
    }
