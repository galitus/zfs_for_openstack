It is based and working on OpenStack Liberty (7.0.1).

There you have a ZFS and CTLadm (FreeBSD) based driver for OpenStack Cinder.

But i guess you can use ZFS without CTLadm. 

Never fully tested.

So it is a generic driver for every OS which can handle ZFS volumes.

Also made some changes on Nova for using libiscsi with qemu and not a host based iscsi daemon.
