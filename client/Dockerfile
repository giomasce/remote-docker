FROM debian:unstable

RUN printf "deb http://deb.debian.org/debian unstable main contrib non-free\ndeb-src http://deb.debian.org/debian unstable main contrib non-free\n" > /etc/apt/sources.list
RUN apt-get update && apt-get install -y eatmydata
RUN eatmydata apt-get update && eatmydata apt-get dist-upgrade -y
RUN eatmydata apt-get update && eatmydata apt-get install -y build-essential sudo apt-utils sudo dpkg-dev ca-certificates gnupg openssh-client coreutils
RUN eatmydata adduser --disabled-password --gecos "User,,," user
ADD gpg_key.asc run.sh /root/
RUN apt-key add /root/gpg_key.asc
RUN gpg --import /root/gpg_key.asc

CMD bash /root/run.sh
