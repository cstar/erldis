task :default => :build

if File.file?('erlang_config.rb') 
  require 'erlang_config'  
else
  puts "erlang_config.rb file is missing."
  puts "You need to fill it with your local configuration."
  puts "An sample has been generated for you."
  File.open("erlang_config.rb",'w') do |file|
    file.write("ERL_TOP=\"<path to your erlang installation>\"\n")
    file.write("EMAKE_COMPILE_OPTIONS = []\n")
  end
  exit(-1)
end

task :build do
  sh "#{ERL_TOP}/bin/erl -make"
end

desc "installs in $ERL_TOP/lib/"
task :install =>  [:build] do |t|
   FileList.new('ebin/*.app').each do |dir|
     vsn = extract_version_information("vsn.config","vsn").gsub("\"","")
     name = dir.gsub("ebin/","").gsub(".app","")
     destination =  "#{erlang_home}/lib/#{name}-#{vsn}"
     puts "#{name} will be installed in #{destination}"
     sh "mkdir -p #{destination}"
     %w{ priv ebin docs include }.each do |d|
       sh "cp -R #{d} #{destination}"
     end
   end
end


def erlang_home
    @erlang_home||=IO.popen("#{ERL_TOP}/bin/erl -noinput -noshell -eval 'io:format(code:root_dir()).' -s init stop").readlines[0] 
end
def extract_version_information(file, type)
  informations = []
  IO.foreach(file) { |line|
    informations << $1 if line =~ /\{#{type},(.*)\}/
  }
  informations[0]
end