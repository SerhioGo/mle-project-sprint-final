docker-compose up --build
Builds (if needed) + Starts the services

docker-compose up
Starts services using existing images

docker-compose build
Only builds images, does not start services. Safer since rewrite image no metter what. If new requierements.txt or something - this is an option

docker-compose down
Stops and removes containers (but keeps images)

docker-compose stop
Stops containers without removing them

docker ps -a
List of all containers

docker rm -f $(docker ps -aq)
Stop and remove ALL containers

docker system prune -a
Clean up everythings (containers, images, networks, volumes)

docker container prune
Remove Only Stopped Containers (Safe Cleanup)

docker system prune -a --volumes
Remove volumes
