pipeline {
    agent any

    environment {
        APP_IMAGE = "green-real-backend"
        APP_CONTAINER = "green-real-backend"
        APP_PORT = "5053"
        RUN_ID = "${env.BUILD_TAG}"

        ELECTRICITYMAPS_API_KEY = "BpjVnaE4hWm9CE947xSP"
        MONGO_URI = 'mongodb+srv://admin:admin1234@green-devops-monitor.xxflzzs.mongodb.net/?retryWrites=true&w=majority&appName=Green-DevOps-Monitor'
    }

    stages {

        stage('Build') {
            steps {
                sh '''
                docker run --rm \
                --volumes-from jenkins \
                -e ELECTRICITYMAPS_API_KEY="$ELECTRICITYMAPS_API_KEY" \
                -e MONGO_URI="$MONGO_URI" \
                -w "$WORKSPACE" \
                nikolaik/python-nodejs:python3.12-nodejs20 \
                sh -c "pip install -r requirements.txt && python monitor_runner.py --stage build --run-id $RUN_ID --cmd 'cd real_backend && npm install && npm run build'"
                '''
            }
        }

        stage('Test') {
            steps {
                sh '''
                docker run --rm \
                --volumes-from jenkins \
                -e ELECTRICITYMAPS_API_KEY="$ELECTRICITYMAPS_API_KEY" \
                -e MONGO_URI="$MONGO_URI" \
                -w "$WORKSPACE" \
                nikolaik/python-nodejs:python3.12-nodejs20 \
                sh -c "pip install -r requirements.txt && python monitor_runner.py --stage test --run-id $RUN_ID --cmd 'cd real_backend && npm test'"
                '''
            }
        }

        stage('Deploy') {
            steps {
                sh '''
                docker rm -f $APP_CONTAINER || true

                docker run --rm \
                --volumes-from jenkins \
                -v /var/run/docker.sock:/var/run/docker.sock \
                -v /usr/bin/docker:/usr/bin/docker \
                -e ELECTRICITYMAPS_API_KEY="$ELECTRICITYMAPS_API_KEY" \
                -e MONGO_URI="$MONGO_URI" \
                -e APP_IMAGE="$APP_IMAGE" \
                -e APP_CONTAINER="$APP_CONTAINER" \
                -e APP_PORT="$APP_PORT" \
                -w "$WORKSPACE" \
                nikolaik/python-nodejs:python3.12-nodejs20 \
                sh -c "pip install -r requirements.txt && python monitor_runner.py --stage deploy --run-id $RUN_ID --cmd 'docker build -t $APP_IMAGE ./real_backend && docker run -d --name $APP_CONTAINER -p $APP_PORT:3000 $APP_IMAGE'"
                '''
            }
        }
    }
}