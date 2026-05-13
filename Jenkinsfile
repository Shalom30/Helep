pipeline {
    agent any

    environment {
        DOCKERHUB_CREDENTIALS = credentials('dockerhub-creds')
        DOCKERHUB_USER = 'shalom30'
        KUBECONFIG = credentials('kubeconfig')
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build & Push Images') {
            steps {
                script {
                    def services = ['user-service', 'sos-service', 'dispatch-service', 'notification-service', 'analytics-service']
                    docker.withRegistry('https://index.docker.io/v1/', 'dockerhub-creds') {
                        services.each { svc ->
                            def img = docker.build("${DOCKERHUB_USER}/${svc}:${BUILD_NUMBER}", "services/${svc}")
                            img.push()
                            img.push('latest')
                        }
                    }
                }
            }
        }

        stage('Deploy to Kubernetes') {
            steps {
                sh """
                    helm upgrade --install helep ./helm/helep \
                        --set image.tag=${BUILD_NUMBER} \
                        --namespace default \
                        --wait
                """
            }
        }

        stage('Smoke Test') {
            steps {
                sh """
                    kubectl rollout status deployment/user-service
                    kubectl rollout status deployment/sos-service
                    kubectl rollout status deployment/dispatch-service
                    kubectl rollout status deployment/notification-service
                    kubectl rollout status deployment/analytics-service
                """
            }
        }
    }

    post {
        failure {
            echo 'Pipeline failed — check logs above'
        }
        success {
            echo 'HELEP deployed successfully'
        }
    }
}