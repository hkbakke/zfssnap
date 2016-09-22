Vagrant.configure("2") do |config|
    config.vm.box = "debian/jessie64"
    config.vm.hostname = "zfssnap-dev"
    config.vm.network :private_network, type: "dhcp"
    config.vm.provision :shell, path: "bootstrap.sh"

    config.vm.provider "virtualbox" do |v|
        v.name = "zfssnap-dev"
        v.gui = false
        v.memory = 1024
        v.cpus = 1
        (0..3).each do |d|
            disk_image = ".vagrant/disks/disk-#{d}.vdi"
            unless File.exists?(disk_image)
                v.customize ["createhd", "--filename", disk_image, "--size", "1000"]
            end
            v.customize ["storageattach", :id, "--storagectl", "SATA Controller", "--port", 1+d, "--device", 0, "--type", "hdd", "--medium", disk_image]
        end
    end
end