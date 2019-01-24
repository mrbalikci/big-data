package filtering;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

public class filtering {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 // Parameters
		for (String file :args)
	    {
			getlines(file, file + "2", "something");
	    }

	}

	protected static void getlines (String fname, String oname, String headerText)
	{
		try
		{

			File inFile = new File(fname);

			if (!inFile.isFile())
			{
				System.out.println("Parameter is not an existing file");
				return;
			}

			//Construct the new file that will later be renamed to the original filename.
			File tempFile = new File(oname);

			BufferedReader br = new BufferedReader(new FileReader(inFile));
			PrintWriter pw = new PrintWriter(new FileWriter(tempFile));

			String line = null;

			//Read from the original file and write to the new
			//unless content matches data to be removed.
			while ((line = br.readLine()) != null)
			{
				if (!line.trim().startsWith(headerText))
				{
					pw.println(line);
					pw.flush();
				}
			}
			pw.close();
			br.close();

			/* /Delete the original file
            if (!inFile.delete()) {
                System.out.println("Could not delete file");
                return;
            }

            //Rename the new file to the filename the original file had.
            if (!tempFile.renameTo(inFile))
                System.out.println("Could not rename file");
			 */

		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}

	}

}
