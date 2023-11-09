package tests;
import java.util.Scanner;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;


class TestRunner {
     
	public static void main(String[] args) {
		
		Result result = new Result();
	
		result = JUnitCore.runClasses(SimpleOkafkaProducer.class, SimpleOkafkaConsumer.class);
				//SimpleOkafkaProducer.class
				  
		for (Failure failure : result.getFailures()) {
	        System.out.println("Test failure : "+ failure.toString());
	    }
			System.out.println("Tests ran succesfully: " + result.wasSuccessful());
		    
	   }
   }

