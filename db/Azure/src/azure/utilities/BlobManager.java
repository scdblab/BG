/**                                                                                                                                                                                
 * Copyright (c) 2013 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Ujwala Tambe and Pranit Mhatre                                                                                                                           
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package azure.utilities;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.blob.client.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BlobManager
{
    private static final String storageConnectionString =
            "DefaultEndpointsProtocol=http;"
            + "AccountName=up599;"
            + "AccountKey=IlO2VK/oYHfhwqvlD/f6ohQAc0pwBs+HLp5jeub2b2zkyMDn2Ugvv5ojZX8z6xnxuRhbkBPFHQcN5Vj82dj9pg==";
    private CloudStorageAccount storageAccount;
    private CloudBlobClient blobClient;
    private CloudBlobContainer picContainer, tpicContainer;
    public static final String PIC = "pic";
    public static final String TPIC = "tpic";

    /**
     * Initializer block to load the containers
     */
    {
        try
        {
            // Retrieve storage account from connection-string
            storageAccount = CloudStorageAccount.parse(storageConnectionString);
            // Create the blob client
            blobClient = storageAccount.createCloudBlobClient();
            // Get a reference to a container
            picContainer = blobClient.getContainerReference("pic");
            tpicContainer = blobClient.getContainerReference("tpic");
        }
        catch (URISyntaxException | StorageException | InvalidKeyException ex)
        {
            Logger.getLogger(BlobManager.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Unable to get the BLOB details!");
        }
    }

    public byte[] getBLOB(String containerType, String userid) throws AzureClientException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try
        {
            CloudBlob cloudBlob;
            switch (containerType)
            {
                case PIC:
                    cloudBlob = picContainer.getBlockBlobReference(userid);
                    break;
                case TPIC:
                    cloudBlob = tpicContainer.getBlockBlobReference(userid);
                    break;
                default:
                    throw new AzureClientException("Incorrect argument: " + containerType);
            }
            cloudBlob.download(outputStream);
            return outputStream.toByteArray();
        }
        catch (IOException | URISyntaxException | StorageException ex)
        {
            Logger.getLogger(BlobManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new AzureClientException("Unable to get BLOB information for user " + userid);
        }
    }

    public void putBLOB(String containerType, String userid, byte[] blobContent) throws AzureClientException
    {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(blobContent);
        try
        {
            CloudBlob cloudBlob;
            switch (containerType)
            {
                case PIC:
                    cloudBlob = picContainer.getBlockBlobReference(userid);
                    break;
                case TPIC:
                    cloudBlob = tpicContainer.getBlockBlobReference(userid);
                    break;
                default:
                    throw new AzureClientException("Incorrect argument: " + containerType);
            }
            cloudBlob.upload(inputStream, blobContent.length);
        }
        catch (IOException | URISyntaxException | StorageException ex)
        {
            Logger.getLogger(BlobManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new AzureClientException("Unable to put BLOB information for user " + userid);
        }
    }
    
    public void deleteContainers() throws AzureClientException
    {
        try
        {
            picContainer.delete();
            tpicContainer.delete();
        }
        catch (StorageException ex)
        {
            Logger.getLogger(BlobManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new AzureClientException("Unable to delete the BLOB containers!");
        }
    }
}
