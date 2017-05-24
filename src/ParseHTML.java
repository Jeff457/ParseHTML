import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ParseHTML
{
    private static Map<String, String> bookkeeping = null;
    private static Map<String, String> urlTitles = null;
    private static int urlCount = 0;

    public static void main(String[] args)
    {
        System.out.println("Getting document IDs and URLs...");
        String path = "C:/Users/Jeff/Downloads/webpages_clean/bookkeeping.tsv";
        bookkeeping = parseBookkeeping(path);
        System.out.println("Done.");

        System.out.println("Determining which titles have already been grabbed...");
        path = "titles.tsv";
        urlTitles = parseBookkeeping(path);
        urlCount = (urlTitles == null ? urlCount : urlTitles.size());  // pick up where we left off
        System.out.println("Done.");

        System.out.println("Grabbing titles from URLs...");
        getTitles();
        System.out.println("Done.");

        System.out.println("Inserting into DB...");
        try {
            insertIntoDB();
        } catch (IllegalAccessException | ClassNotFoundException | SQLException | InstantiationException e) {
            e.printStackTrace();
        }
        System.out.println("Done.");
    }

    private static Map<String, String> parseBookkeeping(String path)
    {
        Map<String, String> map = new HashMap<>();

        try(BufferedReader reader = new BufferedReader(new FileReader(path)))
        {
            for(String line; (line = reader.readLine()) != null; )
            {
                String[] docLine = line.split("\\t");
                if (docLine.length < 2)  // would only happen for (docID, title) map in rare cases
                {
                    String url = bookkeeping.get(docLine[0]);  // grab url and truncate if necessary
                    map.put(docLine[0], url.substring(0, (url.length() < 500 ? url.length() : 500)));
                }
                else
                    map.put(docLine[0], docLine[1]);
            }
        } catch (ArrayIndexOutOfBoundsException | IOException e) {
            System.out.println(path + " doesn't exist");
        }

        return map;
    }

    private static void getTitles()
    {
        final int NTHREADS = 100;
        ExecutorService pool = Executors.newFixedThreadPool(NTHREADS);
        bookkeeping.forEach((docId, url) -> pool.submit(() -> {
            try {
                if (urlTitles != null && urlTitles.containsKey(docId))
                    return;  // only grab titles we don't have

                URL validatedURL = validateURL(url);  // in case we need to get the relative path
                Document doc = Jsoup.connect(validatedURL.toString()).get();
                String title = doc.title();
                if (title.isEmpty() || title.startsWith("??"))
                    title = validatedURL.getPath();
                appendToFile(docId, title);  // save to file
            } catch (IllegalArgumentException | IOException e) {
                System.out.println("Bad URL, attempting to save path as title");
                String[] path = url.split(".edu/");

                try {
                    appendToFile(docId, path[1].substring(0, (path[1].length() < 500 ? path[1].length() : 500)));  // use part of URL as title instead

                } catch(ArrayIndexOutOfBoundsException outOfBounds) {
                    System.out.println("Couldn't save path as title, saving URL as title");
                    appendToFile(docId, url.substring(0, (url.length() < 500 ? url.length() : 500)));
                    return;
                }
                return;
            }
        }));
        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.out.println("unable to terminate pool");
        }
    }

    private static URL validateURL(String url) throws MalformedURLException
    {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            return new URL("http://" + url);
        }
    }

    private static void insertIntoDB() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
    {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        Connection connection =	DriverManager.getConnection("jdbc:mysql://localhost:3306/invertedindex?autoReconnect=true&useSSL=false","mytestuser", "mypassword");
        connection.setAutoCommit(false);

        String updateString = "INSERT INTO docid_url(doc_id,url,title) VALUES(?,?,?);";
        PreparedStatement insert_docid_url_title = connection.prepareStatement(updateString);

        bookkeeping.forEach((docID, url) -> {
            try {
                insert_docid_url_title.setString(1, docID);
                insert_docid_url_title.setString(2, url);
                insert_docid_url_title.setString(3, urlTitles.get(docID));
                insert_docid_url_title.executeUpdate();
            } catch (SQLException e) {e.printStackTrace();}
        });

        connection.commit();
        connection.close();
    }

    private static void appendToFile(String docID, String title)
    {
        String outputFile = "titles.tsv";
        String output = docID + "\t" + title;
        BufferedWriter writer = null;
        urlTitles.put(docID, title);

        try {
            File file = new File(outputFile);

            if (!file.exists())
                file.createNewFile();

            writer = new BufferedWriter(new FileWriter(file, true));
            writer.write(output);
            writer.newLine();
            urlCount++;

            System.out.println("Finished retrieving " + urlCount + "/" + bookkeeping.size() + " titles");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (writer != null)
                    writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
