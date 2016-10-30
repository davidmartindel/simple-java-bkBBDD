package bkBBDD;

import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.LineNumberReader;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Scanner;


public class Main {
	static final String DIR="c:\\Temp\\backup\\";
	public static void main(String args[]) {
		
		
		
		Calendar c = Calendar.getInstance();
		c.setFirstDayOfWeek(Calendar.MONDAY);
		int dayOfWeek = (c.get(Calendar.DAY_OF_WEEK)-1);
		FileOutputStream file;
		try {
			file = new FileOutputStream(DIR+dayOfWeek+"\\output.log");
			TeePrintStream tee = new TeePrintStream(file, System.out);
			System.setOut(tee);
			System.setErr(tee);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("  ");
		System.out.println("  Separator -> CODE ALT+21");
		System.out.println("  ");
		System.out.println(" ______  _    _           ______  ______  _____   _____   ");
		System.out.println("(____  \\| |  / )         (____  \\(____  \\(____ \\ (____ \\");  
		System.out.println(" ____)  ) | / /    ___    ____)  )____)  )_   \\ \\ _   \\ \\"); 
		System.out.println("|  __  (| |< <    (___)  |  __  (|  __  (| |   | | |   | |");
		System.out.println("| |__)  ) | \\ \\          | |__)  ) |__)  ) |__/ /| |__/ /"); 
		System.out.println("|______/|_|  \\_)         |______/|______/|_____/ |_____/");
		System.out.println("   ");
		System.out.println("_______________________________________________________");
		System.out.println("   ");
		System.out.println("   ");
		int width = 80;
		int height = 30;

	        //BufferedImage image = ImageIO.read(new File("/Users/mkyong/Desktop/logo.jpg"));
		BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
		Graphics g = image.getGraphics();
		g.setFont(new Font("SansSerif", Font.PLAIN, 24));

		Graphics2D graphics = (Graphics2D) g;
		graphics.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING,
					RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
		DateFormat dateFormat = new SimpleDateFormat("dd-MM");
		graphics.drawString(dateFormat.format(c.getTime()), 10, 20);
		System.out.println(c.getTime());

		System.out.println("   ");
		for (int y = 0; y < height; y++) {
			StringBuilder sb = new StringBuilder();
			for (int x =10; x < width; x++) {

				sb.append(image.getRGB(x, y) == -16777216 ? " " : "#");
					
			}

			if (sb.toString().trim().isEmpty()) {
				continue;
			}

			System.out.println(sb);
		}
		
	
		

		
		File sourceLoc=new File(DIR+dayOfWeek);
		int dayOfWeekPre=dayOfWeek-1;
		if(dayOfWeekPre<1)
			dayOfWeekPre=5;
		boolean isFolderExisted=false;
		isFolderExisted=(sourceLoc.exists()==true?sourceLoc.isDirectory()==true?true:false:false);
		if(isFolderExisted==false){
			System.out.println("     ERROR");
			System.out.println("  +-------------------------+");
			System.out.println("  ! no existe el directorio !");
			System.out.println("  !  C:\\Temp\\backup\\"+dayOfWeek+"       ! ");
			System.out.println("  !                         ! ");
			System.out.println("  +-------------------------+ ");
			System.out.println("   ");
	        System.out.println("Presione la tecla ENTER para cerrar la ventana");
	        Scanner waitForKeypress = new Scanner(System.in);
	        waitForKeypress.nextLine();
		}else{
		try {
			 	FileWriter statsFile = new FileWriter(DIR+dayOfWeek+"\\stats.txt");
	    		GetTableBBDD data = new GetTableBBDD();
	    		
	    		for (String s: args) {
	    			System.out.println("============================================");
	    			data.getTable(s);
	    			try {
	    				//stats
	    				statsFile.write(compare(DIR+dayOfWeekPre+"\\"+s+".csv" , DIR+dayOfWeek+"\\"+s+".csv",s));
	    				statsFile.flush();
					} catch (Exception e) {
						System.out.println("EROOR" + e.toString());
					}
	    			
	            }
	    		statsFile.close();
	    		
		} catch (Exception e) {
			e.printStackTrace();
		}
		}
	}
	
	
	public static String compare(String stringfile1,String stringfile2,String table) {
		long startTime = System.nanoTime();
	  File file1 = new File(stringfile1);
	  File file2 = new File(stringfile2);
	  if (file1 == null || file2 ==null || file1.exists()==false  || file2.exists() ==false)
		  return table+" NO FICHERO: "+stringfile1+ " - " + stringfile2+ "\n";
	try {
	  if(file1.length() ==  file2.length())
		  return "0% NO MODIFICA " + table + " \n";
	  LineNumberReader lnr = new LineNumberReader(new FileReader(file2));
	  lnr.skip(Long.MAX_VALUE); 
	  double totalMax =(long) lnr.getLineNumber();
	  lnr.close();
	 
	  BufferedReader br1 = new BufferedReader(new FileReader(file1)) ;
	  BufferedReader br2 = new BufferedReader(new FileReader(file2)) ;
	  String line1pas="";
	  String line2pas="";
	  	String line1;
	  	String line2;
	  	double dimereturn=(long) 0;
	  	double error=(long) 0;
		while (((line1 = br1.readLine()) != null ) && ((line2 = br2.readLine()) != null )) {
			if(!line1.equals(line2)){
				error++;
				if(line2pas.equals(line1))
					line1 = br1.readLine();
				if(line2pas.equals(line2))
					line2 = br2.readLine();
			}
			dimereturn++;
			line1pas=line1;
			line2pas=line2;
		}
		DecimalFormat df = new DecimalFormat("#.###");		
		//logs
		double finishtime=((System.nanoTime() - startTime)/ 1000000000.0);
		System.out.println("STATS of "+table+" :"+df.format(finishtime)+"seconds ");
		
	  return df.format((error/dimereturn)*100) +"% "+" ERRORES: "+error+"/"+dimereturn + " : "+table +" Nuevas lineas:" + (dimereturn-totalMax)+"  \n";

	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	return "\nERROR "+table+"\n\n";
	  
	}
}
