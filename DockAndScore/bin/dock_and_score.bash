#!/bin/bash

#SETUP
source /scratch/work/project/dd-20-11/toolchain/environment.sh 

originDir=$PWD
geodockBinDir=/scratch/work/project/dd-20-11/geodock/bin/
xscoreBinDir=/scratch/work/project/dd-20-11/xscore_v1.3/bin/

#directory con i source file della proteina, delle pockets file Xscore
srcFiles=/scratch/work/project/dd-20-11/data/srcFiles/

#needed by XSCORE
export XSCORE_PARAMETER=/scratch/work/project/dd-20-11/xscore_v1.3/parameter/

ligandName=$1
proteinName=$2

#CREATE DIRECTORY
mkdir xscoreOut
mkdir GeoDockPoses
mkdir srcFiles
mkdir log

#PREPARE SOURCE FILES
cp $srcFiles/ligands/${ligandName}.mol2 srcFiles/
cp $srcFiles/proteins/$proteinName/* srcFiles/
cp $srcFiles/xtool/* srcFiles/ 

#MODIFY FILENAMES FOR GEODOCK 
mv srcFiles/pocket_key_site.pdb srcFiles/${ligandName}_key_site.pdb
mv srcFiles/${ligandName}.mol2 srcFiles/${ligandName}_crystal.mol2

#RUN GEODOCK
$geodockBinDir/geodock_utils dock -d srcFiles/ --codename $ligandName &> log/geodock.log
echo "$geodockBinDir/geodock_utils dock -d srcFiles/ --codename $ligandName &> log/geodock.log"

#GEODOCK PRODUCES ALL POSES IN DOCKED_POSES.MOL2 IN THE CURRENT DIR
mv docked_poses.mol2 GeoDockPoses/

#RUN XSCORE
$xscoreBinDir/xscore srcFiles/score.input &> log/xscore.log
head --lines 2 xscoreOut/xscore.table > bestPose_XScore.log 

cat bestPose_XScore.log
