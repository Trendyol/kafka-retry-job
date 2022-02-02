
REPOSITORY_ID=$(curl -s -H "PRIVATE-TOKEN: $GITLAB_API_TOKEN" "$CI_API_V4_URL/projects/$CI_PROJECT_ID/registry/repositories" | jq -r '.[0].id');

echo "REPOSITORY_ID: $REPOSITORY_ID";

IMAGE_REPOSITORY=$(curl -s -H "PRIVATE-TOKEN: $GITLAB_API_TOKEN" "$CI_API_V4_URL/registry/repositories/$REPOSITORY_ID?tags=true&tags_count=true");

IMAGE_EXISTS=$(echo $IMAGE_REPOSITORY | jq -r '[.tags[].name | contains("'"$VERSION"'")] | any');

if [[ $IMAGE_EXISTS == "true" ]]; then
  echo "Image with version: $VERSION exists.";
  exit 1;
fi;
