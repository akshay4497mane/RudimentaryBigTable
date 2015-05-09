#include <bits/stdc++.h>
#include <unistd.h>
#include <time.h>//#include <sys/types.h>
#include "mpi.h"
#include "parse.h"
#include <omp.h>
using namespace std;


map <string, pair<string, string> > meta_map;
map<string, int> host2rankmap;         // hostname to world_rank
map<string,bool> hostAvailability;
int world_size;

void initialize(){
    std::ifstream infile("meta.data");
    string a, b, c;
    while (infile >> a >> b >> c)
    {  
        meta_map[a] = make_pair(b, c);
    }
    return;   
}

//for sorting map based on values
template<typename A, typename B>
std::pair<B,A> flip_pair(const std::pair<A,B> &p)
{
    return std::pair<B,A>(p.second, p.first);
}

template<typename A, typename B>
std::multimap<B,A> flip_map(const std::map<A,B> &src)
{
    std::multimap<B,A> dst;
    std::transform(src.begin(), src.end(), std::inserter(dst, dst.begin()), 
                   flip_pair<A,B>);
    return dst;
}



pair<string, string> scheduler(){
    map <string, int> host2file_count;
    for(map<string, int>::iterator it = host2rankmap.begin(); it != host2rankmap.end(); it++ ){
        host2file_count[it->first] = 0;
    }
    for(map<string, pair<string, string> >::iterator it = meta_map.begin(); it != meta_map.end(); it++ ){
        host2file_count[(it->second).first] = host2file_count[(it->second).first] + 1;
        host2file_count[(it->second).second] = host2file_count[(it->second).second] + 1;
    }
    /*string a, c;
    int b = INT_MAX, d = INT_MAX;
    for(map<string, int>::iterator it = host2file_count.begin(); it != host2file_count.end(); it++ ){
        if(it->second < b){
            b = it->second;  //freq
            a = it->first;
        }
    }
     host2file_count[a] = host2file_count[a] + 1;
    for(map<string, int>::iterator it = host2file_count.begin(); it != host2file_count.end(); it++ ){
        if(it->second < d){
            d = it->second;
            c = it->first;
        }
    }*/

 //   std::map<int, double> src;
    string a = "", b = "";
    multimap<int, string> dst = flip_map(host2file_count);

    for (map<int, string>::reverse_iterator it = dst.rbegin(); it != dst.rend(); ++it)
    {
        if(hostAvailability[it -> second] == 1){
            if(a == "")a = it->second;
            if(b == "")b = it->second;    
        }

        if( a != "" && b != "")break;    
    }      

    return make_pair(a, b);  //potential clients
}

void read_file_gfs( string file_name){
    char* hostname1;
    char* hostname2;
    if (meta_map.find(file_name) != meta_map.end()) {
        hostname1 = (char *)(meta_map[file_name].first).c_str();
        hostname2 = (char *)(meta_map[file_name].second).c_str();
    }  
    else {string key = "not_found";
        for (map<string, pair<string, string> >::iterator it = meta_map.begin();it != meta_map.end(); ++it) {
            if ((it->first).find(file_name) != std::string::npos) {
                key = it->first;
                file_name = key;
                break;
            }
        }
        if (key == "not_found") {
            cout << "NO RECORD FOUND.\n";
            return;
        }
        hostname1 = (char *)(meta_map[file_name].first).c_str();
        hostname2 = (char *)(meta_map[file_name].second).c_str();
                //exit(0);                
    }
    cout<<hostname2<<" "<<hostname1<<" "<<file_name;
                    
    if(hostAvailability[hostname1])
    { 
        int target_rank = host2rankmap[string(hostname1)];
        cout <<"\nAvailable from " << target_rank << endl;
        MPI_Send((char *)("r" + file_name).c_str(), 50, MPI_CHAR, target_rank, 9, MPI_COMM_WORLD);
                      //MPI_Send(proc_list, 50, MPI_CHAR, 0, world_rank, MPI_COMM_WORLD);
        int status = -1;
                      //int tag = target_rank;
        MPI_Recv(&status, 1, MPI_INT, target_rank, 91, MPI_COMM_WORLD, MPI_STATUS_IGNORE);//non-blocking
        cout<<"\ndone " << status << endl    ;
    }else if(hostAvailability[hostname2])
    { 
        int target_rank = host2rankmap[string(hostname2)];
        cout <<"\nAvailable from " << target_rank << endl;
        MPI_Send((char *)("r" + file_name).c_str(), 50, MPI_CHAR, target_rank, 9, MPI_COMM_WORLD);
                      //MPI_Send(proc_list, 50, MPI_CHAR, 0, world_rank, MPI_COMM_WORLD);
        int status = -1;
                      //int tag = target_rank;
        MPI_Recv(&status, 1, MPI_INT, target_rank, 91, MPI_COMM_WORLD, MPI_STATUS_IGNORE);//non-blocking
        cout<<"\ndone " << status << endl    ;
    }
    else{
        cout << "FILE NOT AVAILABLE DUE TO NODE FAILURE OF " << hostname1 << "  " << hostname2 << endl;
    }

                  //handle error and read from hostname2
}


void write_file_gfs( string file_name){
    pair<string,string> target_ranks = scheduler();
    meta_map[file_name] = target_ranks;
    std::ofstream outmetafile;
    outmetafile.open ("meta.data", std::ofstream::out | std::ofstream::app);
    outmetafile << file_name + " " + target_ranks.first + " " << target_ranks.second << endl;
                            
    register int c = 0;
    MPI_Send((char *)("w" + file_name).c_str() , 20, MPI_CHAR, host2rankmap[target_ranks.first], 9, MPI_COMM_WORLD);
    if(system((char *)("scp " + file_name + " " + target_ranks.first + ":BIGTABLE" ).c_str())){
                                //RETRY MECHANISM AND ERROR RECOVERY
        c++;    
    }
    else{
        cout << "SUCCESSFULL file written to client " + target_ranks.first << endl;                              
    }

                            //int status;   
                            //MPI_Recv(&status, 1, MPI_INT, host2rankmap[target_ranks.first], 9, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
          
                             //cout << status << endl; 
    MPI_Send((char *)("w" + file_name).c_str() , 20, MPI_CHAR, host2rankmap[target_ranks.second], 9, MPI_COMM_WORLD);
    if(system((char *)("scp " + file_name + " " + target_ranks.second + ":BIGTABLE" ).c_str())){
    //RETRY MECHANISM AND ERROR RECOVERY  
        c++;    
    }
    else{
        cout << "SUCCESSFULL file written to client " + target_ranks.second << endl;                              
    }

    if(c == 2){
        cout << "UNSUCCESSFULL file written to any client " << endl;                                      
    }

}

// checking client server is down or not
void updateHostAvailability(){
    for(map<string, int>::iterator it = host2rankmap.begin(); it != host2rankmap.end(); it++ ){
        if (hostAvailability.find(it->first) != hostAvailability.end()) {
            string cmd = "scp meta.data "+ it->first + ":";
            if (system((char*)cmd.c_str())  ){
                hostAvailability[it->first] = 0;
                  //cout<<""  
            }
            else
                {hostAvailability[it->first] = 1;}
        } 
        else
            {hostAvailability[it->first] = 0;
        } 
    }
}

int main(int argc, char *argv[]){
	ios_base::sync_with_stdio(false);
	const bool poll = true;
  	MPI_Init(&argc, &argv);
  	
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  	int op;	
  	string file_name;

  	if(world_rank == 0){
        initialize();
  		int name_len; 
        char proc_list[MPI_MAX_PROCESSOR_NAME];
          MPI_Get_processor_name(proc_list, &name_len);
          host2rankmap[string(proc_list)] = 0; 
          for(int i = 1;i < world_size; i++){
  			char proc[50];
			int tag = i;

			MPI_Recv(proc, 50, MPI_CHAR, tag, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		   host2rankmap[string(proc)] = i;	
      // hostAvailability	
  		}
  		for(map<string, int>::iterator it = host2rankmap.begin(); it != host2rankmap.end(); it++){
            char *key = (char *)(it->first).c_str();
            int value = it->second;
            MPI_Bcast(key, strlen(key)+1, MPI_CHAR, 0, MPI_COMM_WORLD);
            MPI_Bcast(&value, 1, MPI_INT, 0, MPI_COMM_WORLD);

        }
  	}
  	else{// CLIENT CODE
      int name_len;	
  		char proc_list[MPI_MAX_PROCESSOR_NAME];
  		MPI_Get_processor_name(proc_list, &name_len);
  		MPI_Send(proc_list, 50, MPI_CHAR, 0, world_rank, MPI_COMM_WORLD);

          for(register int i = 0;i<world_size;i++){
            char key[50];
            int value;
            MPI_Bcast(key, 50, MPI_CHAR, 0, MPI_COMM_WORLD);
            MPI_Bcast(&value, 1, MPI_INT, 0, MPI_COMM_WORLD);
            host2rankmap[string(key)] = value;    
        }	
  	}

     MPI_Barrier(MPI_COMM_WORLD);
     cout<<"\nFINISHED FIRST BARRIER\n";


  	if(world_rank == 0){
  		while(poll){
            updateHostAvailability(); 
  			cout<<"\n******** MENU *********";
  			cout<<"\n1. Read file GFS";
  			cout<<"\n2. Write file into GFS";
  			cout<<"\n3. Add New Table into Bigtable";
            cout<<"\n4. Insert Record:";
            cout<<"\n5. READ a Record:";
            cout<<"\n9. Exit\nEnter Choice:";
            cin >> op;	
  			switch(op)
  			{    
                case 1:
  				{	
                    cout<<"\nEnter FileName to Read :";
                    cin >> file_name;
                    read_file_gfs(file_name);
                              
  				}	
                break;
  				case 2:
                {   cout<<"\nEnter FileName to Write :";
                    cin >> file_name;
                    write_file_gfs(file_name);
                             //MPI_Send((char*)file_name)
                             //char target_proc[50];
                             //MPI_Recv(target_proc, 50, MPI_CHAR, target_rank, target_rank, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                             //return
                             //char *hostname = metafile_search(ram, filename);
                                //system("scp "+ ""); 
                             //int target_rank = host2rankmap[string(hostname)];
                             //MPI_Send((char *)("R" + filename).c_str(), 50, MPI_CHAR, target_rank, 0, MPI_COMM_WORLD);
                    }    
                    break;
  				case 3: //Add new table into BIGTABLE
                {
                    string tablename;
                    cout<<"\nTable Name:";
                    cin>>tablename;

               //system("mkdir -p BIGTABLE/"+ tablename );

                }break;
                case 4: 
                {   
                    string tablename,rowidpath,rowid;
                    cout<<"\n Tell Table Name:";
                    cin>>tablename; 
                    cout<<"\nTell FilePath/filename.html to be used as rowid :";
                    cin>>rowidpath;

              
                    string temp=string(rowidpath.c_str());
                    char * pch;
                      string s;
                      pch = strtok ((char *)temp.c_str(),"/");
                      while (pch != NULL)
                        {
                        s = string(pch);
                        pch = strtok (NULL, "/");
                      }
                      rowid=s;
                      time_t t1;
                      struct tm* var1;
                      time(&t1);
                      var1 = localtime(&t1);
                      s = string(asctime(var1));
                      std::replace( s.begin(), s.end(), ' ', '.'); // replace all 'x' to 'y'
                      std::replace( s.begin(), s.end(), ':', '.'); // replace all 'x' to 'y'
                      
                      string newfname = tablename+"_"+rowid+"_"+string(s);
                      cout<<"\n\n\n "<<newfname;
                      string str = newfname;

                      str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
                      str.erase(std::remove(str.begin(), str.end(), '\n'), str.end());
                      newfname=str;
                      
                      //string newfname = "akshay.txt";
                      std::ofstream newfnameoutfile;
                    newfnameoutfile.open (newfname.c_str(), std::ofstream::out);
                    newfnameoutfile << "START_COLUMN_CONTENT"<<"\n";
                    std::ifstream infile(rowidpath.c_str());
                    std::string line;
                    while (std::getline(infile, line))
                    {newfnameoutfile<<line<<endl;
                    }
                    newfnameoutfile << "END_COLUMN_CONTENT"<<"\n\n";

                      anchor* links = parse_html(rowidpath.c_str());

                      newfnameoutfile << "START_COLUMN_OUTLINKS\n\n";

                      while (strlen(links->linktext)) {
                        newfnameoutfile << links->link << "\t" << links->linktext << endl;
                        ++links;
                      }
                      newfnameoutfile << "END_COLUMN_OUTLINKS\n\n";
                      infile.close();
                      newfnameoutfile.close();     
                      write_file_gfs(newfname);

                    } break;
                case 5: //may be delete
                {   
                    string tablename,rowidpath,rowid;
                    cout<<"\n Tell Table Name:";
                    cin>>tablename; 
                    cout<<"\nEnter rowid :";
                    cin>>rowid;
                    string myfname = tablename+"_"+rowid;
                          //cout << myfname << endl;
                          //exit(0);
                    read_file_gfs(myfname);              
                    } break;
                case 9: //may be delete
                { MPI_Abort(MPI_COMM_WORLD, 1);
                } break;
            }
    //     updateHostAvailability(); 
  		}
  	}
  	else{//CLIENT    CLIENT
            std::ofstream outmetafile;
            outmetafile.open ("meta.debug", std::ofstream::out | std::ofstream::app);
            string hnames[world_size];
        for(map<string, int>::iterator it = host2rankmap.begin(); it != host2rankmap.end(); it++){
            hnames[it->second] = it->first;
            outmetafile << "\nMAP::"<<it->second <<" "<<it->first;
        } 

  		while(1){
               char msg[50];
               MPI_Recv(msg, 50, MPI_CHAR, 0, 9, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
               
               outmetafile << "\nHAHAHA"<<"-AKSHAY:::"<<msg;

               if(tolower(msg[0]) == 'w'){
                    //string file = string(msg).substr(1);
                    
                   // int status = 1;
                    //MPI_Send((char *)("w" + file_name).c_str() , 20, MPI_CHAR, host2rankmap[target_ranks.second], 9, MPI_COMM_WORLD);
                            
                    //meta_map[file] = hnames[world_rank]; // updating meta_map which is to be updated in meta file while exiting
                    //MPI_Send()
               }
               else if(tolower(msg[0]) == 'r'){
                    outmetafile<<"AKSHAY:::INSIDE CLIENT READ";
                    string file = string(msg).substr(1);      
                    //system((char *)("scp " + file + " " + hnames[0] + ":"));
                    //system((char *)("scp " + file_name + " " + hnames[target_rank] + ":" ).c_str());
                    outmetafile << "\nEXECUTING SCP::: "<<file<<" "<<hnames[0]<<endl;   
                    string cmd = "scp BIGTABLE/" +  file + " " + hnames[0] + ":";
                    if (system((char*)cmd.c_str())  ){
                         outmetafile << "\nERROR EXECUTING SCP\n";
                         int status = 0;
                         outmetafile <<"\nSEND STATUS ::"<<status;
                         MPI_Send(&status, 1, MPI_INT, 0, 91, MPI_COMM_WORLD);
                    }
                    else{
                         outmetafile << "\nSCP SUCCESS.\n";
                         int status = 1;
                         outmetafile <<"\nSEND STATUS ::"<<status;
                         MPI_Send(&status, 1, MPI_INT, 0, 91, MPI_COMM_WORLD);    
                    }
                 //int name_len; 
                 //char proc[MPI_MAX_PROCESSOR_NAME];
                 //MPI_Get_processor_name(proc, &name_len);
                 //MPI_Send(proc, 50, MPI_CHAR, 0, world_rank, MPI_COMM_WORLD);            
               outmetafile << "\nFINISHED R";
               outmetafile.close();
               } 
               else{
                   // fprintf("");
               } 
               
  		}

  	}
	MPI_Finalize();
}
