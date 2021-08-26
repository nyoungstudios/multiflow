FROM gitpod/workspace-full

# installs oh my zsh
ARG RUNZSH="no"
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"

ENV SHELL='/bin/zsh'
