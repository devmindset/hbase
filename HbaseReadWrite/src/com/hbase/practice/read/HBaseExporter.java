package com.hbase.practice.read;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseExporter {
	private final String SEPARATOR = "\t";
	private final String COMMA_SEPARATOR = ",";
	String tableName;

	HBaseExporter(String tableName) {
		this.tableName = tableName;
	}

	public static void main(String[] args) throws IOException {

		String tableName = args[0];
		String filePath = args[1];
		if (args.length != 2) {
			System.out
					.println("Usage: HBaseExporter <tablename> <csv file path>");
		}
		HBaseExporter exporter = new HBaseExporter(tableName);
		exporter.exportToFile(filePath);

	}

	public void readFile(String path) {
		BufferedReader br = null;

		try {

			String sCurrentLine;

			br = new BufferedReader(new FileReader(path));

			while ((sCurrentLine = br.readLine()) != null) {
				System.out.println("HELLO ----" + sCurrentLine);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	public void exportToFile(String filePath) {
		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();

		// Configuration conf = HBaseConfiguration.create();
		//
		// conf.set("hbase.master", "lei.hadoop.local:60000");
		// conf.set("hbase.zookeeper.quorum", "localhost");
		// conf.set("hbase.cluster.distributed", "true");

		ResultScanner scanner = null;
		try {
			// Map<String,HashMap<String,String>> rows = new
			// HashMap<String,HashMap<String,String>>();
			List<String> rows = new ArrayList<String>();
			// Instantiating HTable class
			HTable table = new HTable(config, this.tableName);
			// Instantiating the Scan class
			Scan scan = new Scan();
			// Getting the scan result
			scanner = table.getScanner(scan);
			Result result = null;
			int linecounter = 0;
			int maxline = 0;
			int fileCounter = 0;
			File file = new File(filePath + "hbasedata_" + fileCounter);
			System.out.println("After blank File Creation");
			while ((result = scanner.next()) != null && maxline <= 2) {
				// System.out.println(result);
				// Reading values from Result class object
				
				int count = 0;
				StringBuilder sb = new StringBuilder();
				for (KeyValue kv : result.raw()) {
					if ((new String(kv.getFamily())
							.equalsIgnoreCase("s")) 
							&& !(new String(kv.getQualifier())
							.equalsIgnoreCase("csv"))) {
						if (count == 0) {
							// System.out.print(new String(kv.getRow()) + " ");
							sb.append(new String(kv.getRow()));
							sb.append(SEPARATOR);
							count++;
						} else if(count==1) {
							sb.append(COMMA_SEPARATOR);
						}
						sb.append(new String(kv.getQualifier()));
					}
				}

				if(null!=sb && !sb.toString().trim().isEmpty()){
				// System.out.println(sb.toString());
				rows.add(sb.toString());
				linecounter++;
				maxline++;

				if (linecounter == 1) {
					System.out.println("Before Append " + file.getName());
					FileWriter writer = null;
					try {
						writer = new FileWriter(file, true);
						for (String str : rows) {
							System.out.println("Before Writing :: ");
							writer.write(str);
							writer.write(System.getProperty("line.separator"));
						}
						//System.out.println("After writing " + file.getName());
						//readFile(filePath + "hbasedata_" + fileCounter);
					} catch (IOException e) {
						System.out
								.println("Exception while writing ArrayList ");
						e.printStackTrace();
					} finally {
						rows.clear();
						linecounter =0;
						if (null != writer) {
							writer.close();
							writer = null;
						}
					}

					if (maxline % 2 == 0) {
						fileCounter++;
						file = new File(filePath + "hbasedata_" + fileCounter);
					}
				}
			}
			}
		} catch (IOException e) {

		} finally {
			try {
				if (null != scanner) {
					scanner.close();
					scanner = null;
				}
			} catch (Exception e) {

			}
		}

	}
}