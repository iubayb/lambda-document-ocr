import { DocumentProcessorServiceClient } from '@google-cloud/documentai';
import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';

// Initialize the S3 client
const s3 = new S3Client();

// Function to process documents with Google Document AI and save the output to S3
export async function handler(event) {
    const bucketName = event.Records[0].s3.bucket.name;
    const documentKey = event.Records[0].s3.object.key;

    const projectId = process.env.GOOGLE_PROJECT_ID;
    const location = process.env.GOOGLE_LOCATION;
    const processorId = process.env.GOOGLE_PROCESSOR_ID;

    const credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS);

    // Initialize the Document AI client
    const client = new DocumentProcessorServiceClient({
        credentials
    });

    // Get the document from S3 using AWS SDK v3
    const documentObject = await s3.send(new GetObjectCommand({
        Bucket: bucketName,
        Key: documentKey,
    }));

    // Convert the document stream to a buffer
    const content = (await streamToBuffer(documentObject.Body)).toString('base64');

    // Construct the processor name
    const name = `projects/${projectId}/locations/${location}/processors/${processorId}`;

    // Determine the MIME type of the document
    const mimeType = determineMimeType(documentKey);

    // Call Document AI to process the document
    const request = {
        name,
        rawDocument: {
            content,
            mimeType,
        }
    };
    const [result] = await client.processDocument(request);
    const text = result.document.text;

    // Save the extracted text back to S3 using AWS SDK v3
    const putObjectParams = {
        Bucket: bucketName,
        Key: `processed_text/${documentKey.replace(/\.[^/.]+$/, '')}.txt`,
        Body: text,
        ContentType: 'text/plain'
    };
    await s3.send(new PutObjectCommand(putObjectParams));

    console.log(`Extracted Text saved to ${putObjectParams.Key}`);
    return { statusCode: 200, body: JSON.stringify(`Document processed and saved successfully to ${putObjectParams.Key}.`) };
};

// Helper function to determine MIME type based on file extension
function determineMimeType(filename) {
    const extension = filename.split('.').pop().toLowerCase();
    switch (extension) {
        case 'pdf':
            return 'application/pdf';
        case 'tiff':
        case 'tif':
            return 'image/tiff';
        case 'jpg':
        case 'jpeg':
            return 'image/jpeg';
        case 'png':
            return 'image/png';
        case 'bmp':
            return 'image/bmp';
        case 'gif':
            return 'image/gif';
        default:
            return 'application/octet-stream'; // Default case if the file type is not recognized
    }
}

// Helper function to convert a readable stream to a buffer
async function streamToBuffer(stream) {
    const chunks = [];
    for await (const chunk of stream) {
        chunks.push(chunk instanceof Buffer ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks);
}
