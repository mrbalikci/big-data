package test_xml;

import java.io.File;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import net.sf.saxon.TransformerFactoryImpl;

public class testing_xml 
{
	
	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		 // Parameters
		/*Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://127.0.0.1:8020");
		try
		{
			FileSystem fs = FileSystem.get(conf);
			fs.close();
		}
		catch (Exception excp)
		{
			excp.toString();
		}*/
		for (String file :args) 
	    {
			process_file(file);			
	    }
		
   
	}
	protected static  void process_file(String file)
	{
		File input = new File(file);
		if (!input.isDirectory())
		{
			
            if (file.endsWith(".xml")) 
            // xslt is the file will get the needed values in column format 
			{
				Properties prop = new Properties();
				prop.setProperty("xslTemplate",
						"C:/Users/ebalikci/Documents/everis/data/data_samples/ny/ny_voltages/Something.xslt");
				Logger logger = Logger.getLogger("danny");
				XMLtransformer(logger, prop, file, file.replace(".xml", ".csv"));
			}
		}
		else
		{
			for(File file_i : input.listFiles())
			{
				process_file(file_i.getAbsolutePath());
			}
		}
		
	}
	
	
	 /**
     * Method transforming a given xml file according to xslt template
     *
     * @param LOGGER logger instance to provide feedback when something goes wrong
     * @param prop properties map with the params to process the xml file
     * @param xmlFile input xml file to be transformed
     * @param outputFile output file with the transformations
     */
	protected static void XMLtransformer(Logger LOGGER, Properties prop, String xmlFile, String outputFile) 
    {
        try 
        {
            //TransformerFactory factory = TransformerFactory.newInstance();
        	TransformerFactory factory = new TransformerFactoryImpl();
            Source xslt = new StreamSource(new File(prop.getProperty("xslTemplate")));
            Transformer transformer = factory.newTransformer(xslt);

            Source text = new StreamSource(new File(xmlFile));
            
            transformer.transform(text, new StreamResult(new File(outputFile)));
        } catch (TransformerConfigurationException e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
        } catch (TransformerException e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
        }
    }	
}
