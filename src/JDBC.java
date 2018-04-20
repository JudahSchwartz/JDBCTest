import sun.nio.ch.ThreadPool;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class JDBC {
    public static void main(String[] args) throws SQLException, InterruptedException {

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            System.out.println("Driver Successfully Loaded!");
            String driver = "jdbc:sqlserver:";

            String url = "mco364.cbjmpwcdjfmq.us-east-1.rds.amazonaws.com";
            String port = "1433";
            String username = "un";
            String password = "pass";
            String database = "SCHWARTZ";
            String connection = String.format(
                    "%s//%s:%s;databaseName=%s;user=%s;password=%s;",
                    driver, url, port, database, username, password);
            try (Connection connect = DriverManager.getConnection(connection)) {
                System.out.println("Connected to Database!");
               // createNamesTable(connect);
                Set<String> names = randomNames(100_000);
                insertNames(names, connect);
                ResultSet rs = findNamesWithTwoOrMoreVowels(connect);
                while (rs.next()) {
                    System.out.println(rs.getString("id"));
                    System.out.println(rs.getString("Name"));
                }
                System.out.println(deleteMoreThanFiveLettersAndTwoVowels(connect) + " rows deleted");




            }
            System.out.println("Database Closed!");
        } catch (ClassNotFoundException ex) {
            System.out.println("Error: Driver Class not found.");
            ex.printStackTrace();
        } catch (SQLException sqlex) {
            System.out.println("Error: SQL Error");
            sqlex.printStackTrace();

        }
    }

    private static int deleteMoreThanFiveLettersAndTwoVowels(Connection connect) throws SQLException {
        PreparedStatement query = connect.prepareStatement("DELETE FROM NAMES WHERE Name LIKE  \'%[AEIOUaeiou]%[AEIOUaeiou]%\' AND " +
                "Len(Name)>5" );
        return query.executeUpdate();
    }

    private static ResultSet findNamesWithTwoOrMoreVowels(Connection connect) throws SQLException {
        PreparedStatement query = connect.prepareStatement("SELECT * FROM NAMES WHERE Name LIKE \'%[AEIOUaeiou]%[AEIOUaeiou]%\' ");
        return query.executeQuery();
    }

    private static void insertNames(Set<String> names, Connection connect) throws SQLException, InterruptedException {
        final String sql = "insert into names(name) values (?)";

        // System.out.println(joiner.toString());
        //maybe could be faster if spliced the set and used threads?

        PreparedStatement query = null;
        query = connect.prepareStatement(sql);
        for (String s : names) {
            query.setString(1, s);
            query.addBatch();

        }

        query.executeBatch();



    }



    private static Set<String> randomNames(int numNames) {
        Set<String> names = ConcurrentHashMap.newKeySet();

        ExecutorService threadpool = Executors.newFixedThreadPool(1000);
        Random rand = new Random();
        List<Character> letters = new ArrayList<>();
        for (int i = 'a'; i < 'z'; i++) {
            letters.add((char) i);
        }
        for (int i = 'A'; i < 'Z'; i++) {
            letters.add((char) i);
        }
            //syncronize on rand?

            for(int i = 0; i < numNames; i++) {
                threadpool.submit(new Runnable() {
                    @Override
                    public void run() {
                        boolean tryAgain;
                        do {
                            StringBuffer s = new StringBuffer();
                            int nameLength = rand.nextInt(10) + 1;
                            for (int i = 0; i < nameLength; i++) {


                                char c = letters.get(rand.nextInt(letters.size()));
                                s.append(c);

                            }
                            tryAgain = !names.add(s.toString());
                        }while(tryAgain);
                    }

                });
            }

        threadpool.shutdown();
        try {
            threadpool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return names;
    }

    private static void createNamesTable(Connection connect) throws SQLException {
        PreparedStatement query = connect.prepareStatement(
                "CREATE TABLE Names (\n" +
                        "    Name VARCHAR(10) ,\n" +
                        "    id int IDENTITY);"

        );
        query.executeUpdate();
    }
}