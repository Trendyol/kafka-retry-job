##  Introduction

Kafka Retry Job is an open-source project for moving Kafka messages from error topics to retry topics, developed by Trendyol with love ![:orange_heart:](https://a.slack-edge.com/production-standard-emoji-assets/13.0/apple-medium/1f9e1.png)

#### Why?

In event-driven architecture, anything can happen. Sometimes our consumers fail to do their work due to network issues, business validations etc. and we need to retry that specific message. So we developed our own tool, which has proven to be very useful, and we decided to share it with the community.

#### How?

Kafka Retry Job is a Kubernetes CronJob. When the job starts, it selects all topics from the Kafka cluster and filters out the error topics via Regex. After that, it gets the messages by partition and moves them to the retry topics.

###  Features

- Dockerizability: Can work in a Docker container.
- Configurability: The user can specify error topics via Regex. Also can specify Error Suffix and Retry Suffix.
- Scalability: The project can work in parallel, so you can deploy to various machine for availablity etc.
- Schedulability: Can work at the given interval.

### Configs

Here is the explanation of environment variables. Please note that the config properties are case sensitive.

- BootstrapServers: Kafka Cluster servers. You can give multiple servers with comma
- Topic Regex: Regex for filtering error topics
- Error Suffix: The suffix of error topics
- Retry Suffix: The suffix of retry topics
- GroupId: GroupId for Retry Job Consumer

## Getting Started

To use Kafka Retry Job, there is a Cron Job yaml file under "example" folder. With the Cron Job yaml file, you can deploy to Kubernetes cluster.

## Roadmap

See the [open issues]([https://github.com/github_username/repo_name/issues](https://github.com/github_username/repo_name/issues)) for a list of proposed features (and known issues).

## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. Any contributions you make are greatly appreciated.

1. Fork the Project

2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)

3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)

4. Push to the Branch (`git push origin feature/AmazingFeature`)

5. Open a Pull Request

## License

Distributed under the MIT License. See [`LICENSE`](https://choosealicense.com/licenses/mit/) for more information.

## Contact

Nuri Doğaç Akyıldız - [Github](https://github.com/dogac00) - [ndakyildiz@gmail.com](mailto:ndakyildiz@gmail.com)

Mehmet Fırat Kömürcü - [Github](https://github.com/MehmetFiratKomurcu) - [mehmetfiratkomurcu@hotmail.com](mailto:mehmetfiratkomurcu@hotmail.com)