import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.UnsupportedCharsetException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

public class S3toElasticConnector implements RequestHandler<S3Event, String> {
	public String Handler(S3Event event, Context context) throws Exception {
		try {
			String bucket = (event.getRecords().get(0)).getS3().getBucket().getName();
			String key = URLDecoder.decode((event.getRecords().get(0)).getS3().getObject().getKey(), "UTF-8");
			String elasticURL = System.getenv("elasticURL");
			String elasticRegion = System.getenv("elasticRegion");
			System.out.println("esURL is ->" + elasticURL + "bucket ->" + bucket + " key ->" + key + " elasticRegion ->"
					+ elasticRegion);

			readFromS3(bucket, key, elasticURL, elasticRegion);
		} catch (IOException e) {
			System.out.println("Expetion in Handler ::" + e);
		}
		return String.valueOf("");
	}

	public String handleRequest(Integer arg0, Context arg1) {
		return null;
	}

	public void readFromS3(String bucketName, String key, String elasticURL, String elasticRegion) throws Exception {
		S3Object fullObject = null;
		S3Object objectPortion = null;
		S3Object headerOverrideObject = null;
		String s3URL = "";
		try {
			AmazonS3 s3Client = (AmazonS3) ((AmazonS3ClientBuilder) AmazonS3ClientBuilder.standard()
					.withRegion(elasticRegion)).build();
			fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
			s3URL = s3Client.getUrl(bucketName, key).toString();
			extractTextOfDocument(fullObject.getObjectContent(), key, elasticURL, s3URL);
		} catch (AmazonServiceException e) {
			System.out.println("Expetion in readFromS3 AmazonServiceException ::" + e);
		} catch (SdkClientException e) {
			System.out.println("Expetion in readFromS3 SdkClientException ::" + e);
		} finally {
			if (fullObject != null) {
				fullObject.close();
			}
			if (objectPortion != null) {
				objectPortion.close();
			}
			if (headerOverrideObject != null) {
				headerOverrideObject.close();
			}
		}
	}

	public void extractTextOfDocument(InputStream fileStream, String key, String elasticURL, String s3URL) {
		String docContent = "";
		Parser parser = new AutoDetectParser();
		Metadata metadata = new Metadata();
		BodyContentHandler handler = new BodyContentHandler(-1);
		PDFParserConfig pdfConfig = new PDFParserConfig();
		try {
			pdfConfig.setExtractInlineImages(true);
			ParseContext parseContext = new ParseContext();
			parseContext.set(PDFParserConfig.class, pdfConfig);
			parseContext.set(Parser.class, parser);
			parser.parse(fileStream, handler, metadata, parseContext);
			docContent = handler.toString();
		} catch (IOException | SAXException | TikaException e) {
			System.out.println("TIKA was not able to exctract text of file ::" + e);
		} finally {
			try {
				fileStream.close();
			} catch (IOException e) {
				System.out.println("TIKA was not able to exctract text of file - file close ::" + e);
			}
		}
		try {
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode objectNode1 = mapper.createObjectNode();
			objectNode1.put("id", key);
			objectNode1.put("DocContent", docContent);
			objectNode1.put("DocTitle", key);
			objectNode1.put("DocProperties", metadata.toString());
			objectNode1.put("DocS3Path", s3URL);

			esIndexing(objectNode1.toString(), key, elasticURL);
		} catch (Exception e) {
			System.out.println("Exepction in Object mapper ::" + e);
		}
	}

	public void esIndexing(String jsonDocument, String key, String elasticURL) {
		System.out.println("inside ES");

		try {
			String encodeKey = URLEncoder.encode(key.replaceAll("\\s", "").replaceAll("/", "+"), "utf-8");
			String elastic_search_url = elasticURL + encodeKey;
			HttpPost httpPost = new HttpPost(elastic_search_url);
			httpPost.setEntity(new StringEntity(jsonDocument, ContentType.APPLICATION_JSON));
			httpPost.setHeader("Accept", "application/json");
			httpPost.setHeader("Content-type", "application/json");
			httpPostRequest(httpPost);
		} catch (UnsupportedCharsetException e) {
			System.out.println("UnsupportedCharsetException in esIndexing ::" + e);
		} catch (UnsupportedEncodingException e) {
			System.out.println("UnsupportedEncodingException in esIndexing ::" + e);
		}
	}

	public static void httpPostRequest(HttpPost httpPost) {
		System.out.println("inside httpPostRequest" + httpPost.toString());
		CloseableHttpClient httpClient = HttpClients.createDefault();
		ResponseHandler<String> responseHandler = new ResponseHandler() {
			public String handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
				int status = response.getStatusLine().getStatusCode();
				if ((status >= 200) && (status < 300)) {
					HttpEntity entity = response.getEntity();
					return entity != null ? EntityUtils.toString(entity) : null;
				}
				System.out.println("its an exception");
				throw new ClientProtocolException("Unexpected response status: " + status);
			}
		};
		try {
			String strResponse = (String) httpClient.execute(httpPost, responseHandler);
			System.out.println("Response ::" + strResponse);
		} catch (Exception e) {
			System.out.println("Exception in httpPostRequest ::" + e);
		}
	}

	@Override
	public String handleRequest(S3Event arg0, Context arg1) {
		return null;
	}

}
