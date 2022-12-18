[![Discord](https://img.shields.io/discord/1037366848737525790?label=Discord&logo=Discord&logoColor=white)](https://dsc.gg/kubectyl)
[![Kubectyl](https://img.shields.io/website?down_message=Offline&label=Discourse&logo=Discourse&up_message=Online&url=https%3A%2F%2Fdiscourse.kubectyl.org)](https://discourse.kubectyl.org/)

## Please note that this is pre-release software; it may contain bugs or corrupt data – it should not be used on a production site.
> Please be sure to back up all files and database content prior to using this pre-release software. <b>Use at your own risk!</b>

## Sponsors

Thank you so very much for your support and contributions to our organization. We are so fortunate to have such generous donors and we can never thank you enough for all the support you've shown us. :sparkling_heart:

[Interested in becoming a sponsor?](https://www.buymeacoffee.com/andrei0465/membership)

| Company | About |
| ------------- | ------------- |
| [**MC Server Hosting LLC**](https://mcserverhosting.net/) | We were the first to create a gaming community that took advantage of Kubernetes, a modern open-source base for us to build our platform on. In addition to our contributions to the Minecraft community, we provide one of the most optimal platforms tailed to gaming while applying our knowledge towards continuous performance improvements. |

## Known issues
- We cannot easily show disk usage of a persistent volume.
- Kubernetes metrics server does not have an implemented method to view network usage.
- Pterodactyl panel nodes and allocations systems must be modified to be compatible with Kubernetes.
- The SFTP server is not yet implemented because most providers offer PVs mounted as ReadWriteOnce.

## Goals of the project
- Work done faster and more efficiently, with less skill requirements;
- Provide more stable (than official) version of Wings;
- Performance optimizations;
- Self-healing servers;
- Kubernetes multi-cluster environments;

## How can I help the project?
Report problems you faced. Merge requests are also welcome :)

## Reporting issues

Please use the [kubectyl/panel](https://github.com/kubectyl/panel) repository to report any issues or make
feature requests for Kuber. In addition, the [security policy](https://github.com/kubectyl/panel/security/policy) listed
within that repository also applies to Kuber.

## License
Special thanks to the [Pterodactyl](https://github.com/pterodactyl) team, we want to express our gratitude for all you have done and the hard work you put into this Open Source software. Code released under the [MIT License](https://github.com/kubectyl/kuber/blob/develop/LICENSE).