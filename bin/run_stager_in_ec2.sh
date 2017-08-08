#!/bin/bash
#
# Run the data-bundle-examples stager (stager.py) on an EC2 server.
# Runs the script in a Docker container to reduce host dependencies.
#

EC2_AMI_ID=ami-cd0f5cb6                                     # Ubuntu Server 16.04 LTS (HVM), SSD Volume Type
INSTANCE_TYPE=m4.2xlarge                                    # VCPUs=8, Network performance=High

grab_aws_creds(){
    echo -n "Ok to use your AWS credentials in ~/.aws/credentials [y/n] ? "
    read answer
    [ ${answer} != 'y' ] && echo "Aborting." && exit 1
    aws_access_key_id=`grep access_key_id ~/.aws/credentials | cut -d' ' -f3`
    aws_secret_access_key=`grep secret_access_key ~/.aws/credentials | cut -d' ' -f3`
}

grab_keypair_name(){
    echo -n "What is the name of the EC2 keypair you wish to use ? "
    read ec2_keypair
}

boot_ec2_instance(){
    echo -e "\n########## Requesting EC2 instance ##########\n"
    block_device_mappings='[ { "DeviceName": "/dev/sda1", "Ebs": { "VolumeSize": 50, "DeleteOnTermination": true } } ]'
    run_instances_response_json=`aws ec2 run-instances --count 1 \
                                                       --block-device-mappings "${block_device_mappings}" \
                                                       --image-id ${EC2_AMI_ID} \
                                                       --instance-type ${INSTANCE_TYPE} \
                                                       --security-groups default inbound-ssh-from-anywhere \
                                                       --key-name ${ec2_keypair} \
                                                       --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=bundle-examples-stager-tmp}]"`
    instance_id=`echo ${run_instances_response_json} | jq -r .Instances[0].InstanceId`
    echo "Instance ID is ${instance_id}"
}

wait_for_ec2_boot_to_complete(){
    echo -e "\n########## Waiting for Boot to Complete ##########"
    aws ec2 wait instance-running --instance-ids ${instance_id}
    describe_instances_response_json=`aws ec2 describe-instances --instance-ids ${instance_id}`
    public_dns=`echo ${describe_instances_response_json} | jq -r .Reservations[0].Instances[0].NetworkInterfaces[0].Association.PublicDnsName`
    echo "Public DNS is $public_dns"
    echo "Waiting for SSH-ability..."
    until ssh -oStrictHostKeyChecking=no -o ConnectTimeout=3 ubuntu@${public_dns} true 2>/dev/null ; do echo -n "."; done
    echo ""
}

terminate_ec2_instance(){
    echo -e "\n########## Terminating EC2 Instance ##########\n"
    terminate_instances_response_json=`aws ec2 terminate-instances --instance-ids ${instance_id}`
    new_state=`echo ${terminate_instances_response_json} | jq .TerminatingInstances[0].CurrentState.Name`
    echo "New state is ${new_state}"
    aws ec2 wait instance-terminated --instance-ids ${instance_id}
}

install_and_setup_docker_on_ec2_instance(){
    echo -e "\n########## Installing Docker ##########\n"
    ssh_ec2 <<-EOF
        set -x
		if [ \`apt-key list | grep Docker | wc -l\` -ne 1 ] ; then
		    echo "Adding docker apt repository"
			curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
		fi
		grep -nq docker /etc/apt/sources.list || \
		sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu \$(lsb_release -cs) stable"
		(dpkg -l | grep -q docker) || sudo apt-get update && sudo apt-get install -y docker-ce htop
		egrep -q "docker:.*ubuntu" /etc/group || sudo usermod -aG docker \${USER}
	EOF
}

setup_git_repo(){
    echo -e "\n########## Setting up Git Repo ########## \n"
    ssh_ec2 <<-EOF
        set -x
        mkdir -p ~/.ssh
        grep -q github.com ~/.ssh/config || echo -e "Host github.com\n\tStrictHostKeyChecking no\n" >> ~/.ssh/config
		[ -d data-bundle-examples ] || git clone --recursive git@github.com:humancellatlas/data-bundle-examples.git
		cd data-bundle-examples
		git checkout spierson-stager
	EOF
}

build_docker_image(){
    echo -e "\n########## Building Docker Image ##########\n"
    ssh_ec2 <<-EOF
        set -x
        cd data-bundle-examples
        cat > Dockerfile <<EODOCKERFILE
FROM python:3.6
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  vim
WORKDIR /code/data-bundle-examples
ADD requirements.txt /code/data-bundle-examples
RUN pip install -r requirements.txt
ENV AWS_ACCESS_KEY_ID=${aws_access_key_id}
ENV AWS_SECRET_ACCESS_KEY=${aws_secret_access_key}
CMD /code/data-bundle-examples/bin/stager.py -j10 --log /code/data-bundle-examples/stager.log
EODOCKERFILE
	docker build . --tag hca-stager
	EOF
}

run_stager(){
    echo -e "\n########## Running Stager ##########\n"
    ssh_ec2 <<-EOF
        set -x
        cd data-bundle-examples
        [ -f import/10x/t_4k/bundles/bundle1/assay.json ] || tar xf import/import.tgz
        # docker run --rm --name hca-stager -v \${HOME}/data-bundle-examples:/code/data-bundle-examples hca-stager
        docker run --name hca-stager -v \${HOME}/data-bundle-examples:/code/data-bundle-examples hca-stager
	EOF
}

ssh_ec2(){
    ssh -oStrictHostKeyChecking=no ubuntu@${public_dns} $*
}

abort(){
    terminate_ec2_instance
    exit 1
}

grab_aws_creds
grab_keypair_name
echo -e "Starting at `date`"
boot_ec2_instance
trap abort SIGINT
wait_for_ec2_boot_to_complete
install_and_setup_docker_on_ec2_instance
setup_git_repo
build_docker_image
run_stager
terminate_ec2_instance
echo -e "Finished at `date`"
exit 0
