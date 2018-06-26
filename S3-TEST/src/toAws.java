import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;

public class toAws {
	
	public static AmazonS3 s3client;
	
	public static void main(String[] args) throws IOException {
		
		s3client = AmazonS3ClientBuilder
				  .standard()
				  .withRegion(Regions.EU_WEST_1)
				  .build();
		
		
		List<Bucket> buckets = s3client.listBuckets();
		for(Bucket bucket : buckets) {
		    System.out.println(bucket.getName());
		}
		
		List<String> keys = getObjectslistFromFolder(buckets.get(0).getName(), "Unsaved");
		
		for(int i = 0; i < keys.size(); ++i) {
			System.out.println(keys.get(i)+" ");
		}
		
		downloadFileFromBucket(buckets.get(0).getName());
		uploadFiletoBucket(buckets.get(0).getName());
		deleteFileFromBucket(buckets.get(0).getName());
		getMetadataFileFromBucket(buckets.get(0).getName());
		
	}
	
	private static void getMetadataFileFromBucket(String bucketName) {
		
		S3Object s3Object = s3client.getObject(bucketName, "Unsaved/2017/10/18/fce039be-774e-40e1-acec-5669b948b70e.csv");
		ObjectMetadata s3ObjectMetadata = s3Object.getObjectMetadata();
		System.out.println(s3ObjectMetadata.getRawMetadata());
	}

	private static void deleteFileFromBucket(String bucketName) {
		
		try {
            s3client.deleteObject(new DeleteObjectRequest(bucketName, "prueba"));
        }
        catch(AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process 
            // it, so it returned an error response.
            e.printStackTrace();
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
	}

	private static void uploadFiletoBucket(String bucketName) throws IOException {
		
        try {
        	PutObjectRequest request = new PutObjectRequest(bucketName, "prueba", new File("C:\\Users\\Lenovo\\Documents\\prueba.csv"));
    		ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("plain/text");
            metadata.addUserMetadata("x-amz-meta-title", "prueba");
            request.setMetadata(metadata);
            s3client.putObject(request);
        }
        catch(AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process 
            // it, so it returned an error response.
            e.printStackTrace();
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
	}

	private static List<String> getObjectslistFromFolder(String bucketName, String folderKey) {
		
		  ListObjectsRequest listObjectsRequest = 
		                                new ListObjectsRequest()
		                                      .withBucketName(bucketName);
		 
		  List<String> keys = new ArrayList<>();
		 
		  ObjectListing objects = s3client.listObjects(listObjectsRequest);
		  for (;;) {
		    List<S3ObjectSummary> summaries = objects.getObjectSummaries();
		    if (summaries.size() < 1) {
		      break;
		    }
		    summaries.forEach(s -> keys.add(s.getKey()));
		    objects = s3client.listNextBatchOfObjects(objects);
		  }
		 
		  return keys;
	}
	
	private static void downloadFileFromBucket(String bucketName) throws IOException {
		
		File file = new File("C:\\Users\\Lenovo\\Documents\\prueba.csv");
		S3Object s3object = s3client.getObject(bucketName, "Unsaved/2017/10/18/fce039be-774e-40e1-acec-5669b948b70e.csv");
		S3ObjectInputStream inputStream = s3object.getObjectContent();
		OutputStream out = new FileOutputStream(file);
		IOUtils.copy(inputStream, out);
		
		if(file.length() > 0) {
			System.out.println("Downloaded file.");
		}
		else {
			System.out.println("File has not been downloaded.");
		}
		
	}
	
}
