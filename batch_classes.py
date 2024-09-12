#!/usr/bin/env python

from subprocess import call
from subprocess import Popen
from subprocess import PIPE
import os

from tree_checker import *
#from fhadd import fhadd


SINGULARITY_IMG = os.path.expandvars("/nfs/dust/cms/user/$USER/slc6_latest.sif")


def write_script(name,workdir,header,sl6_container=False):
    sframe_wrapper=open(workdir+'/sframe_wrapper.sh','w')

    # For some reason, we have to manually copy across certain environment
    # variables, most notably LD_LIBRARY_PATH, and if running on singularity, PATH
    # Note that we need the existing PATH, otherwise it loses basename, sed, etc

    sframe_wrapper.write(
        """#!/bin/bash
cat /etc/redhat-release
echo $APPTAINER_CONTAINER
#source /cvmfs/cms.cern.ch/cmsset_default.sh
#cd /nfs/dust/cms/user/titasroy/Ac_UL/CMSSW_10_6_28
#cmsenv
#source /nfs/dust/cms/user/titasroy/Ac_UL/SFrame/setup.sh
#cd -
#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_STORED:$LD_LIBRARY_PATH
#export PATH=$PATH_STORED:$PATH
WORKDIR=$PWD
source /nfs/dust/cms/user/titasroy/setup_UL_Ac.sh
cd $WORKDIR
# echo "**** BEGIN ENV"
# printenv
# echo "**** END ENV"
#ldd $(which sframe_main)
sframe_main $1
        """)
    sframe_wrapper.close()
    os.system('chmod u+x '+workdir+'/sframe_wrapper.sh')
    if (header.Notification == 'as'):
        condor_notification = 'Error'
    elif (header.Notification == 'n'):
        condor_notification = 'Never'
    elif (header.Notification == 'e'):
        condor_notification = 'Complete'
    else:
        condor_notification = ''

    condor_submitfile_name = workdir+'/CondorSubmitfile_'+name+'.submit'
    if(os.path.isfile(condor_submitfile_name)):
        return

    #Make sure user does not try to submit jobs to the EL7 nodes without singularity from a environment with a SL6 SCRAM_ARCH
    if 'slc6' in os.getenv("SCRAM_ARCH") and not sl6_container:
        raise EnvironmentError("\033[91mSCRAM_ARCH shows this environment is setup for SL6. You tried to submit to EL7 nodes without using a singularity container.\n Make sure to use --sl6container to run these jobs inside singularity container.\033[0m")

    worker_str = ""
    # Run a SLC6 job on EL7 machine using singularity
    if sl6_container:
        if not os.path.isfile(SINGULARITY_IMG):
            print '\033[93m',"Please pull the SLC6 image to your NFS:",'\033[0m'
            print ""
            print '\033[93m','SINGULARITY_CACHEDIR="/nfs/dust/cms/user/$USER/singularity" singularity pull', SINGULARITY_IMG, 'docker://cmssw/slc6:latest','\033[0m'
            print ""
            raise RuntimeError("\033[91mCannot find image, %s. Do not use one from /afs or /cvmfs.\033[0m" % SINGULARITY_IMG)
        worker_str += '+MySingularityImage="'+SINGULARITY_IMG+'"\n'
        worker_str += '+MySingularityArgs="--bind /tmp:/tmp"\n'
    worker_str += '+MySingularityImage="/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/el7:x86_64"\n'
    worker_str += '+MySingularityArgs="--bind /tmp:/tmp"\n'
    submit_file = open(condor_submitfile_name,'w')
    submit_file.write(
        """#HTC Submission File for SFrameBatch
# +MyProject        =  "af-cms"
#Requirements = ( OpSysAndVer == "CentOS7" )
#Requirements = ( OpSysAndVer == "AlmaLinux9" )
""" + worker_str + """
universe          = vanilla
# #Running in local mode with 8 cpu slots
# universe          =  local
# request_cpus      =  8
notification      = """+condor_notification+"""
notify_user       = """+header.Mail+"""
initialdir        = """+workdir+"""
output            = $(Stream)/"""+name+""".o$(ClusterId).$(Process)
error             = $(Stream)/"""+name+""".e$(ClusterId).$(Process)
log               = $(Stream)/"""+name+""".$(Cluster).log
#Requesting CPU and DISK Memory - default +RequestRuntime of 3h stays unaltered
RequestMemory     = """+header.RAM+"""G
RequestDisk       = """+header.DISK+"""G
#You need to set up sframe
getenv            = True
environment       = "LD_LIBRARY_PATH_STORED="""+os.environ.get('LD_LIBRARY_PATH')+""" PATH_STORED="""+os.environ.get('PATH')+""""
JobBatchName      = """+name+"""
executable        = """+workdir+"""/sframe_wrapper.sh
MyIndex           = $(Process) + 1
fileindex         = $INT(MyIndex,%d)
arguments         = """+name+"""_$(fileindex).xml
""")
    submit_file.close()

def resub_script(name,workdir,header,sl6_container=False):
    if (header.Notification == 'as'):
        condor_notification = 'Error'
    elif (header.Notification == 'n'):
        condor_notification = 'Never'
    elif (header.Notification == 'e'):
        condor_notification = 'Complete'
    else:
        condor_notification = ''

    condor_resubmitfile_name = workdir+'/CondorSubmitfile_'+name+'.submit'
    if(os.path.isfile(condor_resubmitfile_name)):
        return

    #Make sure user does not try to submit jobs to the EL7 nodes without singularity from a environment with a SL6 SCRAM_ARCH
    if 'slc6' in os.getenv("SCRAM_ARCH") and not sl6_container:
        raise EnvironmentError("\033[91mSCRAM_ARCH shows this environment is setup for SL6. You tried to submit to EL7 nodes without using a singularity container.\n Make sure to use --sl6container to run these jobs inside singularity container.\033[0m")
    
    worker_str = ""
    # Run a SLC6 job on EL7 machine using singularity
    if sl6_container:
        if not os.path.isfile(SINGULARITY_IMG):
            print '\033[93m',"Please pull the SLC6 image to your NFS:",'\033[0m'
            print ""
            print '\033[93m','SINGULARITY_CACHEDIR="/nfs/dust/cms/user/$USER/singularity" singularity pull', SINGULARITY_IMG, 'docker://cmssw/slc6:latest','\033[0m'
            print ""
            raise RuntimeError("\033[91mCannot find image, %s. Do not use one from /afs or /cvmfs.\033[0m" % SINGULARITY_IMG)
        worker_str += '+MySingularityImage="'+SINGULARITY_IMG+'"\n'
        worker_str += '+MySingularityArgs="--bind /tmp:/tmp"\n'
    worker_str += '+MySingularityImage="/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/el7:x86_64"\n'
    worker_str += '+MySingularityArgs="--bind /tmp:/tmp"\n'
    submitfile = open(condor_resubmitfile_name,'w')
    submitfile.write(
"""#HTC Submission File for SFrameBatch
# +MyProject        =  "af-cms"
# Requirements = ( OpSysAndVer == "AlmaLinux9" )
""" + worker_str + """
universe          = vanilla
# #Running in local mode with 8 cpu slots
# universe          =  local
# request_cpus      =  8
notification      = """+condor_notification+"""
notify_user       = """+header.Mail+"""
initialdir        = """+workdir+"""
output            = $(Stream)/"""+name+""".o$(ClusterId).$(Process)
error             = $(Stream)/"""+name+""".e$(ClusterId).$(Process)
log               = $(Stream)/"""+name+""".$(Cluster).log
#Requesting CPU and DISK Memory - default +RequestRuntime of 3h stays unaltered
# RequestMemory     = """+header.RAM+"""G
RequestMemory     = 8G
RequestDisk       = """+header.DISK+"""G
#You need to set up sframe
getenv            = True
environment       = "LD_LIBRARY_PATH_STORED="""+os.environ.get('LD_LIBRARY_PATH')+""" PATH_STORED="""+os.environ.get('PATH')+""""
JobBatchName      = """+name+"""
executable        = """+workdir+"""/sframe_wrapper.sh
arguments         = """+name+""".xml
queue
""")
    submitfile.close()

def submit_qsub(NFiles,Stream,name,workdir):
    #print '-t 1-'+str(int(NFiles))
    #call(['ls','-l'], shell=True)

    if not os.path.exists(Stream):
        os.makedirs(Stream)
        print Stream+' has been created'

    #call(['qsub'+' -t 1-'+str(NFiles)+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'], shell=True)
    # proc_qstat = Popen(['condor_qsub'+' -t 1-'+str(NFiles)+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'],shell=True,stdout=PIPE)
    # return (proc_qstat.communicate()[0].split()[2]).split('.')[0]
    cmd = ['condor_submit'+' '+workdir+'/CondorSubmitfile_'+name+'.submit'+' -a \'"Stream='+Stream.split('/')[1]+'"\' -a \'"queue '+str(NFiles)+'"\'']
    # print cmd
    proc_qstat = Popen(cmd,shell=True,stdout=PIPE)
    
    output = proc_qstat.communicate()[0]
    # print output
    return (output.split()[7]).split('.')[0]


def resubmit(Stream,name,workdir,header,sl6_container):
    #print Stream ,name
    resub_script(name,workdir,header,sl6_container)
    if not os.path.exists(Stream):
        os.makedirs(Stream)
        print Stream+' has been created'
    #call(['qsub'+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'], shell=True)
    # proc_qstat = Popen(['condor_qsub'+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'],shell=True,stdout=PIPE)
    # return proc_qstat.communicate()[0].split()[2]
    proc_qstat = Popen(['condor_submit'+' '+workdir+'/CondorSubmitfile_'+name+'.submit'+' -a \'"Stream='+Stream.split('/')[1]+'"\''],shell=True,stdout=PIPE)
    return (proc_qstat.communicate()[0].split()[7]).split('.')[0]

def add_histos(directory,name,NFiles,workdir,outputTree, onlyhists,outputdir):
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    FNULL = open(os.devnull, 'w')
    if os.path.exists(directory+name+'.root'):
        call(['rm '+directory+name+'.root'], shell=True)
    string=''
    proc = None
    position = -1
    command_string = 'nice -n 10 hadd ' # -v 1 ' # the -v stopped working in root 6.06/01 now we get a lot of crap
    if onlyhists: command_string += '-T '
    if(outputTree):
        for i in range(NFiles):
            if check_TreeExists(directory+workdir+'/'+name+'_'+str(i)+'.root',outputTree) and position ==-1:
                position = i
                string+=str(i)
                break

    for i in range(NFiles):
        if not position == i and not position == -1:
            string += ','+str(i)
        elif position ==-1:
            string += str(i)
            position = 0

    source_files = ""
    if NFiles > 1:
        source_files = directory+workdir+'/'+name+'_{'+string+'}.root'
    else:
        source_files = directory+workdir+'/'+name+'_'+string+'.root'

    #print command_string+directory+name+'.root '+source_files
    #print outputdir+'/hadd.log'
    if not string.isspace():
        proc = Popen([str(command_string+directory+name+'.root '+source_files+' > '+outputdir+'/hadd.log')], shell=True, stdout=FNULL, stderr=FNULL)
    else:
        print 'Nothing to merge for',name+'.root'
	return proc


