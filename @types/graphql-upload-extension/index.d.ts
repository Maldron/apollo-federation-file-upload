import 'graphql-upload-minimal';

declare module 'graphql-upload-minimal' {
  // eslint-disable-next-line import/prefer-default-export
  export interface Upload {
    promise: Promise<FileUpload>;
  }
}
