package tests;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.junit.BeforeClass;

public class okafkaSetup {
	
    @BeforeClass
    public static Properties setup(){
     
	  final Properties BaseProperties = new Properties();
	  InputStream input;
    	  try {
    		  input = new FileInputStream("config.properties");
              BaseProperties.load(input);
	        } catch (IOException e) {
	        }
	  return BaseProperties;
    }
    
    
}
