#include "publisher.hpp"
#include "consumer.hpp"
#include "s3service.pb.h"
#include "clientsocket.hpp"

#include <iostream>
#include <thread>
#include <cstddef>
#include <experimental/filesystem>
#include <bits/stdc++.h>
#include <vector>

#include <sys/stat.h>
#include <boost/filesystem.hpp>
#include <bits/stdc++.h>
#include <iostream>
#include <cstdint>
#include <boost/filesystem.hpp>

#include <unistd.h>
#include <stdio.h>

#include <string>
#include <termios.h>
using namespace std;

std::string getRequestId()
{
	std::srand(std::time(nullptr));

	return "id" + std::to_string(std::rand());
}

auto readFromFile(clientSocket& cs, const std::string& objectPath, s3service::s3object& objRequest)
{
	std::ifstream iF("/home/sathish/update_10_05_2023/Client/Debug/to_be_uploaded/"+objectPath, std::ios::in | std::ios::binary);

	if (!iF)
	{
		std::cout << "File not found!";
		return false;
	}

	iF.seekg(0, std::ios::end);
	size_t size = iF.tellg();
	iF.seekg(0, std::ios::beg);

	char* data = new char[size];

	iF.read(data, size);
        iF.close();
	auto ret = cs.writeSocket(data, size);

	if (ret != "")
	{
		std::cout << ret << std::endl;
		return false;
	}

	objRequest.set_len(size);

	

	return true;
}

void writeToFile(clientSocket csRead, const std::string& filename, int len)
{
	std::string new_filename=filename;
	
    	size_t time_index = filename.find_last_of(":");
    	size_t length_filename=filename.size();
    	
    	if ((time_index != std::string::npos)&&(length_filename>6))
    	 {

		// get the extension of the file
	    	size_t extension_index = filename.find_last_of(".");
	    	std::string extension;
	    	if (extension_index != std::string::npos) {
			extension = filename.substr(extension_index);
		}
		// create the new filename with extension
		std::ostringstream oss;
	    	oss << filename.substr(0, (extension_index-21)) << extension;
	    	new_filename = oss.str();
    	}
 	std::string current_path= get_current_dir_name();
	std::string full_path=current_path+"/sathish_download";
	boost::filesystem::create_directories(full_path);
        full_path=current_path+"/sathish_download/"+new_filename;
	std::ofstream oF(full_path.c_str(), std::ios::out | std::ios::binary);
	std::vector<char> data;

	auto ret = csRead.readSocket(data, len);

	if (ret != "")
	{
		std::cout << ret << std::endl;
	}

    oF.write(&data[0], len);
    oF.close();
}

std::string getObjectName(const std::string& path)
{
	std::experimental::filesystem::path p(path);

	return p.filename().string();
}

void createAndPublishRequestPacket(publisher* pub)
{
	clientSocket cs("127.0.0.1", 8081);
	cs.createAndConnect();

	while (true)
	{
		s3service::serviceRequestResponse request;

		std::cout << "Which action you wanna take?:" << std::endl;
		std::cout << "1. Account" << std::endl;
		std::cout << "2. User" << std::endl;
		std::cout << "3. AccessKey" << std::endl;
		std::cout << "4. Bucket" << std::endl;
		std::cout << "5. Object" << std::endl;

		int choice;

		std::cin >> choice;

		if (choice == 1)
		{
			request.set_entitytype(s3service::serviceRequestResponse::ACCOUNT);
			int flag = 0;
			auto accountRequest  = request.add_account();
			while(true)
			{
				std::cout << "Which action?:" << std::endl;
				std::cout << "1. Create Account" << std::endl;
				std::cout << "2. Delete Account" << std::endl;
				std::cout << "3. Go back" << std::endl;
				std::cin >> choice;
				
				if (choice == 1)
				{
					accountRequest->set_accop(s3service::s3account::CREATE_ACCOUNT);
					break;
				}
				else if(choice == 2)
				{
					accountRequest->set_accop(s3service::s3account::DELETE_ACCOUNT);
					break;
				}
				else if(choice == 3)
				{
					flag = 1;
					break;
				}
				else 
				{
					std::cout << "Enter the valid option from 1 or 2" << std::endl;
					continue;
				}
			}
			
			if(flag == 1)
			{
				continue;
			}
				
			std::string accountName, password;
			while(true){
				std::cout << "Enter account name: ";
				char name;
				int coun=0;
				name = getc(stdin);
				if(name != '\n')
					accountName+=name;
					int i=0;
					
				do{
					name = getc(stdin);
					if(name == '\n' && coun==0)
						cout << "You have entered new line, Please Enter the valid account name" << endl;
					else if(name != '\n')
					{
						accountName+=name;
						coun=1;
					}

				}while(name!='\n');
				if(coun)
					break;
        		
        		}
			accountRequest->set_accountname(accountName);
			while(true){
				std::cout << "Enter password: ";
				char pwd;
				int cou=0;
			        pwd=getc(stdin);
			        if(pwd!='\n')
					password+=pwd;
				
				do{
					pwd=getc(stdin);
					if(pwd!='\n')
					{
						password+=pwd;
						cou=1;
					}
					if(pwd == '\n' && cou==0)
						cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(pwd!='\n');
				if(cou)
					break;
        		
        		}
			accountRequest->set_password(password);

			if (choice == 1)
			{
				accountRequest->set_accop(s3service::s3account::CREATE_ACCOUNT);
			}
			else if(choice == 2)
			{
				accountRequest->set_accop(s3service::s3account::DELETE_ACCOUNT);
			}
			else 
			{
			}
		}
		else if (choice == 2)
		{
			request.set_entitytype(s3service::serviceRequestResponse::USER);
			int flag = 0;
			auto userRequest  = request.add_user();
			while(true)
			{
				std::cout << "Which action?:" << std::endl;
				std::cout << "1. Create User" << std::endl;
				std::cout << "2. Update User" << std::endl;
				std::cout << "3. Delete User" << std::endl;
				std::cout << "4. Go back" << std::endl;

				std::cin >> choice;
				if (choice == 1)
				{
					userRequest->set_userop(s3service::s3user::CREATE_USER);
					break;
				}
				else if (choice == 2)
				{
					
					std::string newName;
        			while(true){
					std::cout << "Enter New username: ";
					char nameU;
					int cou=0;
					nameU=getc(stdin);
					if(nameU!='\n')
						newName+=nameU;
					
					do{
						nameU=getc(stdin);
						if(nameU!='\n')
						{
							newName+=nameU;
							cou=1;
						}
						if(nameU == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(nameU!='\n');
				if(cou)
					break;
        		
        		}

					userRequest->set_newusername(newName);

					userRequest->set_userop(s3service::s3user::UPDATE_USER);
					break;
				}
				else if(choice == 3)
				{
					userRequest->set_userop(s3service::s3user::DELETE_USER);
					break;
				}
				else if(choice == 4)
				{
					flag = 1;
					break;
				}
				else 
				{
					std::cout << "Enter the valid option from 1 to 3" << std::endl;
					continue;
				}
			}
			if(flag == 1)
			{
				continue;
			}
			std::string accessKey, secretKey;

			std::cout << "Enter Anthorization keys: " << std::endl;
				while(true){
					std::cout << "AccessKey: ";
					char accKey;
					int cou=0;
					accKey=getc(stdin);
					if(accKey!='\n')
						accessKey+=accKey;
					
					do{
						accKey=getc(stdin);
						if(accKey!='\n')
						{
							accessKey+=accKey;
							cou=1;
						}
						if(accKey == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(accKey!='\n');
				if(cou)
					break;
        		
        		}
        		while(true){
					std::cout << "SecretKey: ";
					char secKey;
					int cou=0;
					secKey=getc(stdin);
					if(secKey!='\n')
						secretKey+=secKey;
					
					do{
						secKey=getc(stdin);
						if(secKey!='\n')
						{
							secretKey+=secKey;
							cou=1;
						}
						if(secKey == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(secKey!='\n');
				if(cou)
					break;
        		
        		}

			auto auth = userRequest->add_auth();

			auth->set_accesskey(accessKey);
			auth->set_secretkey(secretKey);

			

			std::string userName;
       		while(true){
					std::cout << "Enter userName: ";
					char Uname1;
					int cou=0;
					Uname1=getc(stdin);
					if(Uname1!='\n')
						userName+=Uname1;
					
					do{
						Uname1=getc(stdin);
						if(Uname1!='\n')
						{
							userName+=Uname1;
							cou=1;
						}
						if(Uname1 == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(Uname1!='\n');
				if(cou)
					break;
        		
        		}

			userRequest->set_username(userName);


		}
		else if (choice == 3)
		{
			request.set_entitytype(s3service::serviceRequestResponse::ACCESSKEY);
			int flag = 0;
			auto keyRequest  = request.add_accesskey();
			std::string accessKey,secretKey;
			while(true)
			{
				std::cout << "Which action?:" << std::endl;
				std::cout << "1. Create Key" << std::endl;
				std::cout << "2. Delete Key" << std::endl;
				std::cout << "3. Change Status" << std::endl;
				std::cout << "4. Get Last Used Time" << std::endl;
				std::cout << "5. Go back" << std::endl;
				std::cin >> choice;
				if (choice == 1)
				{
					keyRequest->set_accessop(s3service::s3accesskey::CREATE_KEY);
					break;
				}
				else if (choice == 2)
				{
					
				while(true){
					std::cout << "Enter accessKeyId: ";
					char accKey;
					int cou=0;
					accKey=getc(stdin);
					if(accKey!='\n')
						accessKey+=accKey;
					
					do{
						accKey=getc(stdin);
						if(accKey!='\n')
						{
							accessKey+=accKey;
							cou=1;
						}
						if(accKey == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(accKey!='\n');
				if(cou)
					break;
        		
        		}

					keyRequest->set_accesskeyid(accessKey);

					keyRequest->set_accessop(s3service::s3accesskey::DELETE_KEY);
					break;
			}
				else if (choice == 3)
				{

				while(true){
					std::cout << "Enter accessKeyId ";
					char accKey;
					int cou=0;
					accKey=getc(stdin);
					if(accKey!='\n')
						accessKey+=accKey;
					
					do{
						accKey=getc(stdin);
						if(accKey!='\n')
						{
							accessKey+=accKey;
							cou=1;
						}
						if(accKey == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(accKey!='\n');
				if(cou)
					break;
        		
        		}
					keyRequest->set_accesskeyid(accessKey);

					

					std::string status;
					while(true){
					std::cout << "Enter new status:(Active, Inactive) ";
					char sta;
					int cou=0;
					sta=getc(stdin);
					if(sta!='\n')
						status+=sta;
					
					do{
						sta=getc(stdin);
						if(sta!='\n')
						{
							status+=sta;
							cou=1;
						}
						if(sta == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(sta!='\n');
				if(cou)
					break;
        		
        		}		
					keyRequest->set_status(status);

					keyRequest->set_accessop(s3service::s3accesskey::CHANGE_KEY_STATUS);
					break;
				}
				else if(choice == 4)
				{
				while(true){
					std::cout << "Enter accessKeyId ";
					char accKey;
					int cou=0;
					accKey=getc(stdin);
					if(accKey!='\n')
						accessKey+=accKey;
					
					do{
						accKey=getc(stdin);
						if(accKey!='\n')
						{
							accessKey+=accKey;
							cou=1;
						}
						if(accKey == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(accKey!='\n');
				if(cou)
					break;
        		
        		}


				        keyRequest->set_accesskeyid(accessKey);

					keyRequest->set_accessop(s3service::s3accesskey::LAST_USED_TIME);
					break;
				}
				else if(choice == 5)
				{
					flag=1;
					break;
				}
				else 
				{
					std::cout << "Enter the valid option from 1 to 4" << std::endl;
					continue;
				}
			}
			if(flag ==1)
			{
				continue;
			}

			

			std::cout << "Enter Anthorization keys: " << std::endl;
			
			if(choice==1){
			while(true){
					std::cout << "Enter accessKeyId ";
					char accKey;
					int cou=0;
					accKey=getc(stdin);
					if(accKey!='\n')
						accessKey+=accKey;
					
					do{
						accKey=getc(stdin);
						if(accKey!='\n')
						{
							accessKey+=accKey;
							cou=1;
						}
						if(accKey == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(accKey!='\n');
				if(cou)
					break;
        		
        		}
        		}
        		while(true){
					std::cout << "SecretKey: ";
					char secKey;
					int cou=0;
					secKey=getc(stdin);
					if(secKey!='\n')
						secretKey+=secKey;
					
					do{
						secKey=getc(stdin);
						if(secKey!='\n')
						{
							secretKey+=secKey;
							cou=1;
						}
						if(secKey == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(secKey!='\n');
				if(cou)
					break;
        		
        		}
			auto auth = keyRequest->add_auth();
			auth->set_accesskey(accessKey);
			auth->set_secretkey(secretKey);

			

			std::string userName;
        		while(true){
					std::cout << "Enter userName: ";
					char uname;
					int cou=0;
					uname=getc(stdin);
					if(uname!='\n')
						userName+=uname;
					
					do{
						uname=getc(stdin);
						if(uname!='\n')
						{
							userName+=uname;
							cou=1;
						}
						if(uname == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(uname!='\n');
				if(cou)
					break;
        		
        		}
			keyRequest->set_username(userName);
		}
		else if (choice == 4)
		{
			request.set_entitytype(s3service::serviceRequestResponse::BUCKET);

			auto bucketRequest  = request.add_bucket();
			int flag = 0;
			while(true)
			{
				std::cout << "Which action?:" << std::endl;
				std::cout << "1. Create Bucket" << std::endl;
				std::cout << "2. Delete Bucket" << std::endl;
				std::cout << "3. Put Bucket Version" << std::endl;
				std::cout << "4. Get Bucket Version" << std::endl;
				std::cout << "5. List Objects of Bucket" << std::endl;  
				std::cout << "6. List Object Versions of Bucket" << std::endl; 
				std::cout << "7. Go back" << std::endl;  
				/*std::cout << "3. Put Bucket Tag" << std::endl;
				std::cout << "4. Get Bucket Tag" << std::endl;
				std::cout << "5. Put Bucket Version" << std::endl;*/
				std::cin >> choice;
				
				if (choice == 1)
				{
					bucketRequest->set_buckop(s3service::s3bucket::CREATE_BUCKET);
					break;
				}
				else if (choice == 2)
				{
					bucketRequest->set_buckop(s3service::s3bucket::DELETE_BUCKET);
					break;
				}
				else if(choice==3)
				{
					bucketRequest->set_buckop(s3service::s3bucket::PUT_BUCKET_VERSION);
					break;
				}
				/*else if (choice == 3)
				{
					std::string key, value;

					std::cout << "Enter tag";

					std::cin >> key;
					std::cin >> value;


					auto tag = bucketRequest->add_tag();

					tag->set_key(key);
					tag->set_value(value);

					bucketRequest->set_buckop(s3service::s3bucket::PUT_BUCKET_TAG);
				}*/
				else if(choice==4)
				{
					bucketRequest->set_buckop(s3service::s3bucket::GET_BUCKET_VERSION);
					break;
				}
				else if(choice==5)
				{
					bucketRequest->set_buckop(s3service::s3bucket::LIST_OBJECT);
					break;
				}
				else if(choice==6)
				{
					bucketRequest->set_buckop(s3service::s3bucket::LIST_OBJECT_VERSIONS);
					break;
				}
				else if(choice == 7)
				{
					flag = 1;
					break;
				}
				/*else if(choice==4)
				{
					bucketRequest->set_buckop(s3service::s3bucket::GET_BUCKET_TAG);
				}*/
				else
				{
					std::cout << "Enter valid option from 1 to 6 " << std::endl;
					continue;
				}
			
			}
			if(flag == 1)
				continue;
			std::string accessKey, secretKey;

			std::cout << "Enter Anthorization keys: " << std::endl;

				
				
				
			while(true){
					std::cout << "AccessKey: ";
					char accKey;
					int cou=0;
					accKey=getc(stdin);
					if(accKey!='\n')
						accessKey+=accKey;
					
					do{
						accKey=getc(stdin);
						if(accKey!='\n')
						{
							accessKey+=accKey;
							cou=1;
						}
						if(accKey == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(accKey!='\n');
				if(cou)
					break;
        		
        		}

        		while(true){
					std::cout << "SecretKey: ";
					char secKey;
					int cou=0;
					secKey=getc(stdin);
					if(secKey!='\n')
						secretKey+=secKey;
					
					do{
						secKey=getc(stdin);
						if(secKey!='\n')
						{
							secretKey+=secKey;
							cou=1;
						}
						if(secKey == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(secKey!='\n');
				if(cou)
					break;
        		
        		}

			auto auth = bucketRequest->add_auth();

			auth->set_accesskey(accessKey);
			auth->set_secretkey(secretKey);

			std::string bucketName;
			
			while(true){
					std::cout << "Enter bucket name: ";
					char buck_name;
					int cou=0;
					buck_name=getc(stdin);
					if(buck_name!='\n')
						bucketName+=buck_name;
					
					do{
						buck_name=getc(stdin);
						if(buck_name!='\n')
						{
							bucketName+=buck_name;
							cou=1;
						}
						if(buck_name == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(buck_name!='\n');
				if(cou)
					break;
        		
        		}
			bucketRequest->set_bucketname(bucketName);


		}
		else if (choice == 5)
		{
			request.set_entitytype(s3service::serviceRequestResponse::OBJECT);
			int flag = 0;
			auto objectRequest  = request.add_object();
			while(true)
			{
				std::cout << "Which action?:" << std::endl;
				std::cout << "1. Put Object" << std::endl;
				std::cout << "2. Delete Object" << std::endl;
				std::cout << "3. Get Object"  << std::endl;
				std::cout << "4. Go back" << std::endl;
				/*std::cout << "4. INIT_MULTIPART_OBJECT"  << std::endl;
				std::cout << "5. PUT_MULTIPART_OBJECT"  << std::endl;
				std::cout << "6. COMPLETE_MULTIPART_OBJECT"  << std::endl;
				std::cout << "7. ABORT_MULTIPART_OBJECT"  << std::endl;
				std::cout << "8. LIST_MULTIPART_OBJECT"  << std::endl;*/
				std::cin >> choice;
				std::string objectName;
				if (choice == 1)
			       {
			       	std::string objectPath1;
			       	
			       	
			       				
			while(true){
					std::cout << "Enter ObjectName: ";
					char Obj_name;
					int cou=0;
					int flag1=0;
					Obj_name=getc(stdin);
					if(Obj_name!='\n')
						objectPath1+=Obj_name;
					
					do{
						Obj_name=getc(stdin);
						if(Obj_name!='\n')
						{
							objectPath1+=Obj_name;
							cou=1;
						}
						if(Obj_name == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(Obj_name!='\n');
				
				auto ret = readFromFile(cs, objectPath1, *objectRequest);
				
				if (ret == false)
				{
					std::cout << "readFromFile failed" << std::endl;
					objectPath1="";
					continue;
				}
				
				else if(cou)
					break;
					

        		
        		}

					//auto ret = readFromFile(cs, objectPath1, *objectRequest);

					//if (ret == false)
					//{
						//std::cout << "readFromFile failed" << std::endl;
						//break;
					//}

					objectRequest->set_objectop(s3service::s3object::PUT_OBJECT);
					objectName = getObjectName(objectPath1);
					objectRequest->set_objectname(objectName);
					break;
			     	}
				else if (choice == 2)
				{
					objectRequest->set_objectop(s3service::s3object::DELETE_OBJECT);
					break;
				}
				else if (choice == 3)
				{
					objectRequest->set_objectop(s3service::s3object::GET_OBJECT);
					break;
					
				}
/////////////////////////////////////////////////////////////////////////////////////////////////
				/*else if(choice==4)

				{
					objectRequest->set_objectop(s3service::s3object::INIT_MULTIPART_OBJECT);
				}
				else if(choice==5)
				{
					auto ret = readFromFile(cs, objectPath, *objectRequest);
					if (ret == false)
					{
						std::cout << "readFromFile failed" << std::endl;
						continue;
					}
					objectRequest->set_objectop(s3service::s3object::PUT_MULTIPART_OBJECT);
				}
				else if(choice==6)
				{
					std::string s="0";
					std::cout << "Enter Size: ";
					std::cin >> s;

					objectRequest->set_multipartlist(0,s);
					objectRequest->set_multipartuniqueid("123457");

					
					objectRequest->set_objectop(s3service::s3object::COMPLETE_MULTIPART_OBJECT);
				}

				else if (choice==7)
				{
					objectRequest->set_objectop(s3service::s3object::ABORT_MULTIPART_OBJECT);

				}
				else if (choice==8)
				{
					objectRequest->set_objectop(s3service::s3object::LIST_MULTIPART_OBJECT);
				}*/
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			      else if(choice == 4)
			      {	
			      		flag = 1;
			      		break;
			      }
			      else 
			      {
					std::cout << "Invalid option Please enter from 1 to 3" << std::endl;
					continue;
	     		      }

		}
	
		if(flag==1)
		{
			continue;
		}
			

			std::string accessKey, secretKey;

			std::cout << "Enter Authorization keys: " << std::endl;
			while(true){
					std::cout << "AccessKey: ";
					char accKey;
					int cou=0;
					accKey=getc(stdin);
					if(accKey!='\n')
						accessKey+=accKey;
					
					do{
						accKey=getc(stdin);
						if(accKey!='\n')
						{
							accessKey+=accKey;
							cou=1;
						}
						if(accKey == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(accKey!='\n');
				if(cou)
					break;
        		
        		}
			
        		///
        		while(true){
					std::cout << "SecretKey: ";
					char secKey;
					int cou=0;
					secKey=getc(stdin);
					if(secKey!='\n')
						secretKey+=secKey;
					
					do{
						secKey=getc(stdin);
						if(secKey!='\n')
						{
							secretKey+=secKey;
							cou=1;
						}
						if(secKey == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(secKey!='\n');
				if(cou)
					break;
        		
        		}

			
			

			auto auth = objectRequest->add_auth();

			auth->set_accesskey(accessKey);
			auth->set_secretkey(secretKey);

			std::string bucketName, objectPath;
			
			while(true){
					std::cout << "Enter bucket name: ";
					char buck_name;
					int cou=0;
					buck_name=getc(stdin);
					if(buck_name!='\n')
						bucketName+=buck_name;
					
					do{
						buck_name=getc(stdin);
						if(buck_name!='\n')
						{
							bucketName+=buck_name;
							cou=1;
						}
						if(buck_name == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(buck_name!='\n');
				if(cou)
					break;
        		
        		}
			objectRequest->set_bucketname(bucketName);
			if(choice!=1)
			{
												
			while(true){
					std::cout << "Enter ObjectName: ";
					char Obj_name;
					int cou=0;
					Obj_name=getc(stdin);
					if(Obj_name!='\n')
						objectPath+=Obj_name;
					
					do{
						Obj_name=getc(stdin);
						if(Obj_name!='\n')
						{
							objectPath+=Obj_name;
							cou=1;
						}
						if(Obj_name == '\n' && cou==0)
							cout << "You have entered new line, Please Enter the valid password" << endl;

				}while(Obj_name!='\n');
				if(cou)
					break;
        		
        		}
				std::string objectName = getObjectName(objectPath);
				objectRequest->set_objectname(objectName);
			}
			
		}

	        else 
	        {
			std::cout << "Invalid option Please enter from 1 to 5" << std::endl;
			continue;
	        }
		auto requestId = getRequestId();

		std::cout << "Request Id: " << requestId << std::endl;

		request.set_requestid(requestId);

		auto serializedProto = request.SerializeAsString();
		
		pub->publishMessage(serializedProto, "test");

		sleep(3);
		
		
	}
}

void displayConsumedMessage(clientSocket& csRead, s3service::serviceRequestResponse response)
{
	auto entityType = response.entitytype();

	std::cout << "Request Id: " << response.requestid() << std::endl;

	if (entityType == s3service::serviceRequestResponse::ACCOUNT)
	{
		for (int i = 0; i < response.account_size(); i++)
		{
			auto accountResponse = response.account(i);

			auto accountOp = accountResponse.accop();

			if (accountOp == s3service::s3account::CREATE_ACCOUNT)
			{
				if (accountResponse.errorinfo_size() == 0)
				{
					std::cout << "Account created successfully" << std::endl;
					std::cout << "AccountId: " << accountResponse.accountid() << std::endl;

					auto key = accountResponse.keys(0);

					std::cout << "AccessKeyId: " << key.accesskeyid() << std::endl;
					std::cout << "SecretKey: " << key.secretkey() << std::endl;
				}
				else
				{
					s3service::errorDetails error = accountResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error while createAccount():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else
			{
				if (accountResponse.errorinfo_size() == 0)
				{
					std::cout << "Account deleted successfully" << std::endl;
				}
				else
				{
					s3service::errorDetails error = accountResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error while deleteAccount():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
		}
	}
	else if (entityType == s3service::serviceRequestResponse::USER)
	{
		for (int i = 0; i < response.user_size(); i++)
		{
			auto userResponse = response.user(i);

			auto userOp = userResponse.userop();

			if (userOp == s3service::s3user::CREATE_USER)
			{
				if (userResponse.errorinfo_size() == 0)
				{
					//userResponse.set_arn("1234");
					std::cout << "User created successfully" << std::endl;
					std::cout << "User Arn: " << userResponse.arn() << std::endl;
					std::cout << "User Id: " << userResponse.userid() << std::endl;
					std::cout << "Test Arn: " << userResponse.test() << std::endl;
					std::cout << "Create Date: " << userResponse.createdate() << std::endl;
				}
				else
				{
					s3service::errorDetails error = userResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error while createUser():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else if (userOp == s3service::s3user::DELETE_USER)
			{
				if (userResponse.errorinfo_size() == 0)
				{
					std::cout << "User deleted successfully" << std::endl;
				}
				else
				{
					s3service::errorDetails error = userResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error deleteUser():"  << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else
			{
				if (userResponse.errorinfo_size() == 0)
				{
					std::cout << "User updated successfully" << std::endl;
				}
				else
				{
					s3service::errorDetails error = userResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error updateUser():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
		}
	}
	else if (entityType == s3service::serviceRequestResponse::ACCESSKEY)
	{
		for (int i = 0; i < response.accesskey_size(); i++)
		{
			auto keyResponse = response.accesskey(i);

			auto keyOp = keyResponse.accessop();

			if (keyOp == s3service::s3accesskey::CREATE_KEY)
			{
				if (keyResponse.errorinfo_size() == 0)
				{
					std::cout << "Key created successfully" << std::endl;
					std::cout << "AccessKey: " << keyResponse.accesskeyid() << std::endl;
					std::cout << "SecretKey: " << keyResponse.secretkey() << std::endl;
					std::cout << "AccessKeySelector:" << keyResponse.accesskeyselector() << std::endl;
					std::cout << "CreationDate: " << keyResponse.createdate() << std::endl;
					std::cout << "Status: " << keyResponse.status() << std::endl;
				}
				else
				{
					s3service::errorDetails error = keyResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error while createKey():"  << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else if (keyOp == s3service::s3accesskey::DELETE_KEY)
			{
				if (keyResponse.errorinfo_size() == 0)
				{
					std::cout << "Key deleted successfully" << std::endl;
				}
				else
				{
					s3service::errorDetails error = keyResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error deleteKey():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else if (keyOp == s3service::s3accesskey::CHANGE_KEY_STATUS)
			{
				if (keyResponse.errorinfo_size() == 0)
				{
					std::cout << "Key status updated successfully" << std::endl;
				}
				else
				{
					s3service::errorDetails error = keyResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error changeKeyStatus():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else
			{
				if (keyResponse.errorinfo_size() == 0)
				{
					std::cout << "Last Used Time: " << keyResponse.lastuseddate() << std::endl;
					std::cout << "ServiceName: " << keyResponse.servicename() << std::endl;
				}
				else
				{
					s3service::errorDetails error = keyResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error getLastUsedTime():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
		}
	}
	else if (entityType == s3service::serviceRequestResponse::BUCKET)
	{
		for (int i = 0; i < response.bucket_size(); i++)
		{
			auto bucketResponse = response.bucket(i);

			auto bucketOp = bucketResponse.buckop();

			if (bucketOp == s3service::s3bucket::CREATE_BUCKET)
			{
				if (bucketResponse.errorinfo_size() == 0)
				{
					std::cout << "Bucket created successfully" << std::endl;
				}
				else
				{
					s3service::errorDetails error = bucketResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error while createBucket():"  << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else if (bucketOp == s3service::s3bucket::DELETE_BUCKET)
			{
				if (bucketResponse.errorinfo_size() == 0)
				{
					std::cout << "Bucket deleted successfully" << std::endl;
				}
				else
				{
					s3service::errorDetails error = bucketResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error deleteBucket():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else if (bucketOp == s3service::s3bucket::PUT_BUCKET_TAG)
			{
				if (bucketResponse.errorinfo_size() == 0)
				{
					std::cout << "Bucket Tag updated successfully" << std::endl;
				}
				else
				{
					s3service::errorDetails error = bucketResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error putBucketTag():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else if(bucketOp == s3service::s3bucket::PUT_BUCKET_VERSION)
			{
				if (bucketResponse.errorinfo_size() == 0)
				{
				 	std::cout << "Version is " <<bucketResponse.version() << std::endl;
				}
				else
				{
					s3service::errorDetails error = bucketResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error putBucketTag():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;

					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			
		        else if(bucketOp == s3service::s3bucket::GET_BUCKET_VERSION)
			{
				if (bucketResponse.errorinfo_size() == 0)
				{
				 	std::cout << "Version is " << bucketResponse.version() << std::endl;
				}
				else
				{
					s3service::errorDetails error = bucketResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error putBucketTag():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;

					std::cout << "Error Code: " << error.errorcode() << std::endl;

					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else if (bucketOp == s3service::s3bucket::LIST_OBJECT_VERSIONS)
			{
				if (bucketResponse.errorinfo_size() == 0)
				{
					std::cout << "List of Object: " << std::endl;
					std::cout << std::endl;
					std::string str=bucketResponse.list_object_versions();
					std::string word = "";
					std::vector<std::string> updateprefix;
					updateprefix.push_back("Object Name: ");
					updateprefix.push_back("Object Size: ");
					updateprefix.push_back("Version Status: ");
					updateprefix.push_back("Version ID: ");
					int index=0;
					int totalobject=0;
   					for (auto x : str)
    					{
        					if (x == ' ')
        					{
            						std::cout << updateprefix[index++] <<  word << std::endl;
           						word = "";
           						if(index%4==0)
           						{
           							totalobject++;
           							index=0;
           							std::cout << std::endl;
           							std::cout << std::endl;
           						}
        					}
        					else {
            						word = word + x;
        					}
    					}
   					std::cout << word << std::endl;
   					std::cout << "\n" << std::endl;
   					std::cout << "Total Object: " << totalobject << std::endl;
				}
				else
				{
					s3service::errorDetails error = bucketResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error listObject():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;

					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else if (bucketOp == s3service::s3bucket::LIST_OBJECT)
			{
				if (bucketResponse.errorinfo_size() == 0)
				{
					std::cout << "List of Object: " << std::endl;
					std::cout << std::endl;
					std::string str=bucketResponse.list_object();
					std::string word = "";
					int index=0;
   					for (auto x : str)
    					{
        					if (x == ' ')
        					{
            						std::cout <<  word << std::endl;
           						word = "";
        					}
        					else {
            						word = word + x;
        					}
    					}
   					std::cout << word << std::endl;
   					std::cout << std::endl;
				}
				else
				{
					s3service::errorDetails error = bucketResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error listObject():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;

					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else
			{
				if (bucketResponse.errorinfo_size() == 0)
				{
					std::cout << "Following are all the tags: " << std::endl;

					for (int i = 0; i < bucketResponse.tag_size(); i++)
					{
						std::cout << "Tag" << i+1 << ":" << std::endl;
						auto tag = bucketResponse.tag(i);

						std::cout << tag.key() << "->" << tag.value() << std::endl;
					}
				}
				else
				{
					s3service::errorDetails error = bucketResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error getLastUsedTime():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}

		}
	}
	else if (entityType == s3service::serviceRequestResponse::OBJECT)
	{
		for (int i = 0; i < response.object_size(); i++)
		{
			auto objectResponse = response.object(i);

			auto objectOp = objectResponse.objectop();

			if (objectOp == s3service::s3object::PUT_OBJECT)
			{
				if (objectResponse.errorinfo_size() == 0)
				{
					std::cout << "Object created successfully" << std::endl;
				}
				else
				{
					s3service::errorDetails error = objectResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error while createObject():"  << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else if (objectOp == s3service::s3object::DELETE_OBJECT)
			{
				if (objectResponse.errorinfo_size() == 0)
				{
					std::cout << "Object deleted successfully" << std::endl;
				}
				else
				{
					s3service::errorDetails error = objectResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error deleteObject():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}
			else if (objectOp == s3service::s3object::GET_OBJECT)
			{
				if (objectResponse.errorinfo_size() == 0)
				{
					std::cout << "Object Retrived successfully" << std::endl;
					writeToFile(csRead, objectResponse.objectname(), objectResponse.len());
				}
				else
				{
					s3service::errorDetails error = objectResponse.errorinfo(0);

					auto errorCode = error.errorcode();

					std::cout << "Encountered an error getBucket():" << std::endl;
					std::cout << "Error Type:" << error.errortype() << std::endl;
					std::cout << "Error Code: " << error.errorcode() << std::endl;
					std::cout << "Error Message: " << error.errormessage() << std::endl;
				}
			}

		}
	}
}

int main()
{
	publisher pub("", "", "", "", "requestExchange");

	auto headers = pub.getHeaders();

	headers.insert({"Content-type", "text/text"});
	headers.insert({"Content-encoding", "UTF-8"});

	pub.addHeaders(headers);

	auto err = pub.init();

	if(err.size() != 0)
	{
		std::cout << "Error: " << err << std::endl;
	    return false;
	}

	err = pub.createAndBindQueue("requestQueue", "test");

	if(err.size() != 0)
	{
		std::cout << "Error: " << err << std::endl;
	    return false;
	}

	consumer cObj("", "", "", "", "responseQueue", "client");

	err = cObj.init();

	if (err.size() != 0)
	{
		std::cout << "Error consumer::init(): " << err << std::endl;
		return false;
	}

	err = cObj.bindToExchange("responseExchange", "test");

	if (err.size() != 0)
	{
		std::cout << "Error consumer::bindToExchange(): " << err << std::endl;
		return false;
	}

	clientSocket csRead("127.0.0.1", 9090);

	 auto ret = csRead.createAndConnect();

	if (ret != "")
	{
		std::cout << "cdRead.createAndConect() failed " << ret << std::endl;
		return -1;
	}

	std::thread th1(&createAndPublishRequestPacket, &pub);

	std::thread th2(&consumer::consumeMessage, &cObj);

	while (true)
	{
		for (size_t i = 0; i < consumer::m_consumeRequestMsg.size(); i++)
		{
			s3service::serviceRequestResponse response;

			response.ParseFromString(consumer::m_consumeRequestMsg[i]);

			std::cout << std::endl;
			std::cout << "*****************************************" << std::endl;
			std::cout << std::endl;

			displayConsumedMessage(csRead, response);

			std::cout << std::endl;
			std::cout << "*****************************************" << std::endl;
			std::cout << std::endl;

			consumer::m_consumeRequestMsg.erase(consumer::m_consumeRequestMsg.begin() + i);
			i--;
		}
	}

	th1.join();
	th2.join();

    return 0;
}
